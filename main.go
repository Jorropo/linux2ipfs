package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"time"

	pb "github.com/Jorropo/linux2ipfs/pb"
	proto "google.golang.org/protobuf/proto"

	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	car "github.com/ipld/go-car"
	mh "github.com/multiformats/go-multihash"

	"go.uber.org/multierr"
	"golang.org/x/sys/unix"
)

const defaultBlockTarget = 1024 * 1024 * 2                  // 2 MiB
const defaultCarMaxSize = 32*1024*1024*1024 - 1024*1024*128 // ~32 GiB
const defaultInlineLimit = 32
const tempFileNamePattern = ".temp.%s.car"
const envKeyKey = "ESTUARY_KEY"
const envShuttleKey = "ESTUARY_SHUTTLE"
const defaultIncrementalFile = "old.json"

var rawleafCIDLength int
var dagPBCIDLength int
var directoryData []byte

func init() {
	h, err := mh.Encode(make([]byte, 32), mh.SHA2_256)
	if err != nil {
		panic(err)
	}
	rawleafCIDLength = len(cid.NewCidV1(cid.Raw, h).Bytes())
	dagPBCIDLength = len(cid.NewCidV1(cid.DagProtobuf, h).Bytes())

	typ := pb.UnixfsData_Directory
	directoryData, err = proto.Marshal(&pb.UnixfsData{
		Type: &typ,
	})
	if err != nil {
		panic(err)
	}

	flag.Usage = func() {
		o := flag.CommandLine.Output()
		fmt.Fprint(o, "Usage for: "+os.Args[0]+` <target file path>

Environ:
  - `+envKeyKey+` estuary API key REQUIRED
  - `+envShuttleKey+` shuttle domain REQUIRED

Positional:
  <target file path> REQUIRED

Flags:
`)
		flag.PrintDefaults()
		fmt.Fprintln(o, "Sample:\n  "+envKeyKey+"=EST...ARY "+envShuttleKey+"=shuttle-4.estuary.tech "+os.Args[0]+" fileToUpload/")
	}
}

func main() {
	os.Exit(mainRet())
}

var blockTarget int64
var carMaxSize int64
var inlineLimit int64

func mainRet() int {
	var incrementalFile string
	var target string
	var concurrentChunkers int64
	estuaryKey := os.Getenv(envKeyKey)
	estuaryShuttle := os.Getenv(envShuttleKey)
	{
		flag.Int64Var(&blockTarget, "block-target", defaultBlockTarget, "Maximum size of blocks.")
		flag.Int64Var(&carMaxSize, "car-size", defaultCarMaxSize, "Car reset point, this is mostly how big you want your CARs to be, but it actually is at which point does it stop adding more blocks to it, there is often a 1~128MiB more data to sent (the fakeroots and the header).")
		flag.Int64Var(&inlineLimit, "inline-limit", defaultInlineLimit, "The maximum size at which to attempt to inline blocks.")
		flag.StringVar(&incrementalFile, "incremental-file", defaultIncrementalFile, "Path to the file which stores the old CIDs and old update time.")
		flag.Int64Var(&concurrentChunkers, "concurrent-chunkers", 0, "Number of chunkers to concurrently run, 0 == Num CPUs (note, this only works intra file, the discovery loop is still single threaded).")
		flag.Parse()

		bad := false

		if estuaryKey == "" {
			fmt.Fprintln(os.Stderr, "error empty "+envKeyKey+" envKey")
			bad = bad || true
		}
		if estuaryShuttle == "" {
			fmt.Fprintln(os.Stderr, "error empty "+envShuttleKey+" envKey")
			bad = bad || true
		}

		if blockTarget < 1024 {
			fmt.Fprintln(os.Stderr, "error block-target should be at least 1024 bytes")
			bad = bad || true
		}
		if blockTarget > 1024*1024*2 {
			fmt.Fprintln(os.Stderr, "error block-target cannot be bigger than 2MiB")
			bad = bad || true
		}
		if carMaxSize < blockTarget {
			fmt.Fprintln(os.Stderr, "error car-size cannot be smaller than block-target")
			bad = bad || true
		}
		if inlineLimit < 0 {
			fmt.Fprintln(os.Stderr, "error inline-limit cannot be negative")
			bad = bad || true
		}
		if incrementalFile == "" {
			fmt.Fprintln(os.Stderr, "error empty incremental-file")
			bad = bad || true
		}
		if concurrentChunkers == 0 {
			concurrentChunkers = int64(runtime.NumCPU())
		}

		if args := flag.Args(); len(args) != 1 {
			fmt.Fprintln(os.Stderr, "error expected one positional <target file path>")
			bad = bad || true
		} else {
			target = args[0]
			l := len(target) - 1
			if target[l] == '/' {
				target = target[:l]
			}
		}

		if bad {
			return 1
		}
	}

	tempFileName := fmt.Sprintf(tempFileNamePattern, "A")
	tempCarA, err := os.OpenFile(tempFileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error openning tempCar: "+err.Error())
		return 1
	}
	defer os.Remove(tempFileName)
	defer tempCarA.Close()

	tempFileName = fmt.Sprintf(tempFileNamePattern, "B")
	tempCarB, err := os.OpenFile(tempFileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error openning tempCar: "+err.Error())
		return 1
	}
	defer os.Remove(tempFileName)
	defer tempCarB.Close()

	r := &recursiveTraverser{
		tempCarChunk:           tempCarA,
		tempCarSend:            tempCarB,
		tempCarOffset:          carMaxSize,
		chunkT:                 make(chan struct{}, 1),
		sendT:                  make(chan sendJobs, 1),
		sendOver:               make(chan struct{}),
		concurrentChunkerCount: concurrentChunkers,
		estuaryKey:             estuaryKey,
		estuaryShuttle:         "https://" + estuaryShuttle + "/content/add-car",
	}
	go r.sendWorker()
	r.chunkT <- struct{}{}

	f, err := os.Open(incrementalFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error openning "+incrementalFile+", if you havn't created it do \"echo {} > "+incrementalFile+"\": "+err.Error())
		return 1
	}
	defer f.Close()

	err = json.NewDecoder(f).Decode(&r.olds)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error decoding incremental: "+err.Error())
		return 1
	}
	err = f.Close()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error closing incremental: "+err.Error())
		return 1
	}
	if r.olds.Cids == nil {
		r.olds.Cids = map[string]*savedCidsPairs{}
	}

	entry, err := os.Lstat(target)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error stating "+target+": "+err.Error())
		return 1
	}
	c, updated, err := r.do(target, entry)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error doing: "+err.Error())
		return 1
	}
	err = r.swap()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error making last swap: "+err.Error())
		return 1
	}
	close(r.sendT)

	fmt.Fprintln(os.Stdout, c.Cid.String())

	r.olds.LastUpdate = time.Now()
	f, err = os.OpenFile(incrementalFile, os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error openning "+incrementalFile+": "+err.Error())
		return 1
	}
	defer f.Close()

	err = json.NewEncoder(f).Encode(&r.olds)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error writing "+incrementalFile+": "+err.Error())
		return 1
	}
	err = f.Close()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error closing incremental: "+err.Error())
		return 1
	}
	if updated {
		fmt.Fprintln(os.Stderr, "updated")
	} else {
		fmt.Fprintln(os.Stderr, "non-updated")
	}

	<-r.sendOver

	return 0
}

func (r *recursiveTraverser) sendWorker() {
	defer close(r.sendOver)
	for task := range r.sendT {
		err := r.send(task)
		if err != nil {
			panic(fmt.Errorf("error sending: %e\n", err))
		}
		r.chunkT <- struct{}{}
	}
}

func (r *recursiveTraverser) send(job sendJobs) error {
	if job.offset == carMaxSize || len(job.roots) == 0 {
		// Empty car do nothing.
		return nil
	}

	cidsToLink := make([]*pb.PBLink, len(job.roots))
	var nameCounter uint64
	for _, v := range job.roots {
		sSize := uint64(v.DagSize)
		n := strconv.FormatUint(uint64(nameCounter), 32)
		cidsToLink[nameCounter] = &pb.PBLink{
			Name:  &n,
			Tsize: &sSize,
			Hash:  v.Cid.Bytes(),
		}
		nameCounter++
	}

	// Write a linking root if we have multiple roots
	var data []byte
	for len(cidsToLink) != 1 {
		// Binary search the optimal amount of roots to link
		low := 2
		high := len(cidsToLink) - 1
		var blockData []byte
		var err error
		for low <= high {
			median := (low + high) / 2

			blockData, err = proto.Marshal(&pb.PBNode{
				Links: cidsToLink[:median],
				Data:  directoryData,
			})
			if err != nil {
				return fmt.Errorf("serialising fake root: %e", err)
			}

			l := int64(len(blockData))
			if l == blockTarget {
				low = median
				goto AfterPerfectSize
			}
			if l < blockTarget {
				low = median + 1
			} else {
				high = median - 1
			}
		}
		{
			// in case we finished by too big estimation, fix them by reserialising the correct amount
			if int64(len(blockData)) > blockTarget {
				blockData, err = proto.Marshal(&pb.PBNode{
					Links: cidsToLink[:low],
					Data:  directoryData,
				})
				if err != nil {
					return fmt.Errorf("serialising fake root: %e", err)
				}
			}
		}
	AfterPerfectSize:
		sSize := uint64(len(blockData))
		for _, v := range cidsToLink[:low] {
			sSize += *v.Tsize
		}
		cidsToLink = cidsToLink[low-1:] // Saving space to overwrite the first element with the new directory

		// Making block header
		varuintHeader := make([]byte, binary.MaxVarintLen64+dagPBCIDLength+len(data)+len(blockData))
		uvarintSize := binary.PutUvarint(varuintHeader, uint64(dagPBCIDLength)+uint64(len(blockData)))
		varuintHeader = varuintHeader[:uvarintSize]

		h := sha256.Sum256(blockData)
		mhash, err := mh.Encode(h[:], mh.SHA2_256)
		if err != nil {
			return fmt.Errorf("encoding multihash: %e", err)
		}
		c := cid.NewCidV1(cid.DagProtobuf, mhash)
		data = append(append(append(varuintHeader, c.Bytes()...), blockData...), data...)
		n := strconv.FormatUint(uint64(nameCounter), 32)
		cidsToLink[0] = &pb.PBLink{
			Name:  &n,
			Tsize: &sSize,
			Hash:  c.Bytes(),
		}
		nameCounter++
	}

	var buff io.Reader
	// Writing CAR header
	{
		c, err := cid.Cast(cidsToLink[0].Hash)
		if err != nil {
			return fmt.Errorf("casting CID back from bytes: %e", err)
		}
		headerBuffer, err := cbor.DumpObject(&car.CarHeader{
			Roots:   []cid.Cid{c},
			Version: 1,
		})
		if err != nil {
			return fmt.Errorf("serialising header: %e", err)
		}

		varuintHeader := make([]byte, binary.MaxVarintLen64+uint64(len(headerBuffer))+uint64(len(data)))
		uvarintSize := binary.PutUvarint(varuintHeader, uint64(len(headerBuffer)))
		buff = bytes.NewBuffer(append(append(varuintHeader[:uvarintSize], headerBuffer...), data...))
	}

	// copying tempCar to out
	err := r.tempCarSend.Sync()
	if err != nil {
		return fmt.Errorf("syncing temp file: %e", err)
	}
	_, err = r.tempCarSend.Seek(job.offset, 0)
	if err != nil {
		return fmt.Errorf("seeking temp file: %e", err)
	}
	req, err := http.NewRequest("POST", r.estuaryShuttle, io.MultiReader(buff, r.tempCarSend))
	if err != nil {
		return fmt.Errorf("creating the request failed: %e", err)
	}

	req.Header.Set("Content-Type", "application/car")
	req.Header.Set("Authorization", "Bearer "+r.estuaryKey)

	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("posting failed: %e", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("non 200 result code: %d / body: %s", resp.StatusCode, string(b))
	}

	return nil
}

func (r *recursiveTraverser) writePBNode(data []byte) (cid.Cid, error) {
	// Making block header
	varuintHeader := make([]byte, binary.MaxVarintLen64+dagPBCIDLength+len(data))
	uvarintSize := binary.PutUvarint(varuintHeader, uint64(dagPBCIDLength)+uint64(len(data)))
	varuintHeader = varuintHeader[:uvarintSize]

	fullSize := int64(len(data)) + int64(uvarintSize) + int64(rawleafCIDLength)

	h := sha256.Sum256(data)
	mhash, err := mh.Encode(h[:], mh.SHA2_256)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("encoding multihash: %e", err)
	}
	fakeLeaf := cid.NewCidV1(cid.Raw, mhash)
	rootBlock := append(append(varuintHeader, fakeLeaf.Bytes()...), data...)
	r.toSend = append(r.toSend, &cidSizePair{
		Cid:      fakeLeaf,
		FileSize: fullSize,
		DagSize:  fullSize,
	})

	off, err := r.takeOffset(fullSize)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("taking offset: %e", err)
	}
	err = fullWriteAt(r.tempCarChunk, rootBlock, off)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("writing root's header: %e", err)
	}

	return cid.NewCidV1(cid.DagProtobuf, mhash), nil
}

type sendJobs struct {
	roots  []*cidSizePair
	offset int64
}

type recursiveTraverser struct {
	tempCarOffset int64
	tempCarChunk  *os.File
	tempCarSend   *os.File

	chunkT   chan struct{}
	sendT    chan sendJobs
	sendOver chan struct{}

	concurrentChunkerCount int64

	estuaryKey     string
	estuaryShuttle string

	toSend []*cidSizePair

	olds struct {
		Cids       map[string]*savedCidsPairs `json:"cids,omitempty"`
		LastUpdate time.Time                  `json:"lastUpdate,omitempty"`
	}

	client http.Client
}

type savedCidsPairs struct {
	Cid     string `json:"cid"`
	DagSize int64  `json:"dagSize"`
}

func (r *recursiveTraverser) pullBlock() sendJobs {
	j := sendJobs{r.toSend, r.tempCarOffset}
	r.toSend = nil
	return j
}

type cidSizePair struct {
	Cid      cid.Cid
	FileSize int64
	DagSize  int64
}

func (r *recursiveTraverser) do(task string, entry os.FileInfo) (*cidSizePair, bool, error) {
	switch entry.Mode() & os.ModeType {
	case os.ModeSymlink:
		old, oldExists := r.olds.Cids[task]
		if oldExists && entry.ModTime().Before(r.olds.LastUpdate) {
			// Recover old link
			c, err := cid.Decode(old.Cid)
			if err != nil {
				return nil, false, fmt.Errorf("decoding old cid \"%s\": %e", old.Cid, err)
			}
			return &cidSizePair{
				Cid:     c,
				DagSize: old.DagSize,
			}, false, nil
		}

		target, err := os.Readlink(task)
		if err != nil {
			return nil, false, fmt.Errorf("resolving symlink %s: %e", task, err)
		}

		typ := pb.UnixfsData_Symlink

		data, err := proto.Marshal(&pb.UnixfsData{
			Type: &typ,
			Data: []byte(target),
		})
		if err != nil {
			return nil, false, fmt.Errorf("marshaling unixfs %s: %e\n", task, err)
		}

		data, err = proto.Marshal(&pb.PBNode{Data: data})
		if err != nil {
			return nil, false, fmt.Errorf("marshaling ipld %s: %e\n", task, err)
		}

		hash, err := mh.Encode(data, mh.IDENTITY)
		if err != nil {
			return nil, false, fmt.Errorf("inlining %s: %e", task, err)
		}

		c := cid.NewCidV1(cid.DagProtobuf, hash)
		var new bool
		if oldExists {
			new = c.String() != old.Cid
		} else {
			new = true
		}
		dagSize := int64(len(data))
		if new {
			r.olds.Cids[task] = &savedCidsPairs{
				Cid:     c.String(),
				DagSize: dagSize,
			}
		}
		return &cidSizePair{
			Cid:     c,
			DagSize: dagSize,
		}, new, nil

	case os.ModeDir:
		old, oldExists := r.olds.Cids[task]
		new := !oldExists || entry.ModTime().After(r.olds.LastUpdate)

		subThings, err := os.ReadDir(task)
		if err != nil {
			return nil, false, fmt.Errorf("ReadDir %s: %e\n", task, err)
		}

		links := make([]*pb.PBLink, len(subThings))

		var dagSum int64
		sCids := make([]*cidSizePair, len(subThings))
		for i, v := range subThings {
			sInfo, err := v.Info()
			if err != nil {
				return nil, false, fmt.Errorf("getting info of %s/%s: %e", task, v.Name(), err)
			}
			sCid, updated, err := r.do(task+"/"+v.Name(), sInfo)
			new = new || updated
			if err != nil {
				return nil, false, err
			}

			sCids[i] = sCid

			dagSum += sCid.DagSize
			sSize := uint64(sCid.DagSize)
			n := v.Name()
			links[i] = &pb.PBLink{
				Name:  &n,
				Tsize: &sSize,
				Hash:  sCid.Cid.Bytes(),
			}
		}

		if !new {
			// Recover old link
			c, err := cid.Decode(old.Cid)
			if err != nil {
				return nil, false, fmt.Errorf("decoding old cid \"%s\": %e", old.Cid, err)
			}
			return &cidSizePair{
				Cid:     c,
				DagSize: old.DagSize,
			}, false, nil
		}

		data, err := proto.Marshal(&pb.PBNode{
			Links: links,
			Data:  directoryData,
		})
		if err != nil {
			return nil, false, fmt.Errorf("can't Marshal directory %s: %e", task, err)
		}
		if int64(len(data)) > blockTarget {
			return nil, false, fmt.Errorf("%s is exceed block limit, TODO: support sharding directories", task)
		}

		dagSum += int64(len(data))

		c, err := r.writePBNode(data)
		if err != nil {
			return nil, false, fmt.Errorf("writing directory %s: %e", task, err)
		}

		if oldExists {
			new = c.String() != old.Cid
		} else {
			new = true
		}
		if new {
			r.olds.Cids[task] = &savedCidsPairs{
				Cid:     c.String(),
				DagSize: dagSum,
			}
		}
		return &cidSizePair{
			Cid:     c,
			DagSize: dagSum,
		}, new, nil

	default:
		// File
		old, oldExists := r.olds.Cids[task]
		if oldExists && entry.ModTime().Before(r.olds.LastUpdate) {
			// Recover old link
			c, err := cid.Decode(old.Cid)
			if err != nil {
				return nil, false, fmt.Errorf("decoding old cid \"%s\": %e", old.Cid, err)
			}
			return &cidSizePair{
				Cid:     c,
				DagSize: old.DagSize,
			}, false, nil
		}

		f, err := os.Open(task)
		if err != nil {
			return nil, false, fmt.Errorf("failed to open %s: %e", task, err)
		}
		defer f.Close()

		var c *cidSizePair
		size := entry.Size()
		// This check is really important and doesn't only deal with inlining
		// This ensures that no zero sized files is chunked (the chunker would fail horribly with thoses)
		if size <= inlineLimit {
			data := make([]byte, size)
			_, err := io.ReadFull(f, data)
			if err != nil {
				return nil, false, fmt.Errorf("reading %s: %e", task, err)
			}
			hash, err := mh.Encode(data, mh.IDENTITY)
			if err != nil {
				return nil, false, fmt.Errorf("inlining %s: %e", task, err)
			}
			c = &cidSizePair{
				Cid:      cid.NewCidV1(cid.Raw, hash),
				FileSize: size,
				DagSize:  size,
			}

		} else {
			blockCount := (size-1)/blockTarget + 1
			chunkSize := size / blockCount
			reminder := size - (chunkSize * blockCount)
			CIDs := make([]*cidSizePair, blockCount)
			manager := &concurrentChunkerManager{
				concurrentChunkerCount: r.concurrentChunkerCount,
				t:                      make(chan struct{}, r.concurrentChunkerCount),
				e:                      make(chan error),
			}
			manager.populate()
			var sentCounter int64

			var fileOffset int64
			for i := int64(0); i != blockCount; i++ {
				workSize := chunkSize
				if i < reminder {
					workSize++
				}

				varuintHeader := make([]byte, binary.MaxVarintLen64+rawleafCIDLength)
				uvarintSize := binary.PutUvarint(varuintHeader, uint64(rawleafCIDLength)+uint64(workSize))
				varuintHeader = varuintHeader[:uvarintSize]

				blockHeaderSize := uvarintSize + rawleafCIDLength
				fullSize := int64(blockHeaderSize) + workSize

				carOffset, needSwap := r.mayTakeOffset(fullSize)

				if needSwap {
					err := manager.waitForAllChunks()
					if err != nil {
						return nil, false, err
					}
					r.toSend = append(r.toSend, CIDs[sentCounter:i]...)
					sentCounter += i - sentCounter
					err = r.swap()
					if err != nil {
						return nil, false, fmt.Errorf("swapping: %e", err)
					}
					manager.populate()
					r.tempCarOffset -= fullSize
					carOffset = r.tempCarOffset
				}

				err := manager.getChunkToken()
				if err != nil {
					return nil, false, err
				}
				go r.mkChunk(manager, f, task, &CIDs[i], varuintHeader, int64(blockHeaderSize), workSize, carOffset, fileOffset)
				fileOffset += workSize
			}

			err := manager.waitForAllChunks()
			if err != nil {
				return nil, false, err
			}
			r.toSend = append(r.toSend, CIDs[sentCounter:]...)

			if len(CIDs) == 0 {
				panic("Internal bug!")
			}

			for len(CIDs) != 1 {
				// Generate roots
				var newRoots []*cidSizePair
				for len(CIDs) != 0 {
					if len(CIDs) == 1 {
						// Don't create roots that links to one block, just forward that block
						newRoots = append(newRoots, CIDs...)
						break
					}

					var CIDCountAttempt int = 2
					var fileSum int64 = int64(CIDs[0].FileSize) + int64(CIDs[1].FileSize)
					var dagSum int64 = int64(CIDs[0].DagSize) + int64(CIDs[1].DagSize)
					lastRoot, err := makeFileRoot(CIDs[:CIDCountAttempt], uint64(fileSum))
					if err != nil {
						return nil, false, fmt.Errorf("building a root for %s: %e", task, err)
					}
					for len(CIDs) > CIDCountAttempt {
						fileSum += CIDs[CIDCountAttempt].FileSize
						CIDCountAttempt++
						newRoot, err := makeFileRoot(CIDs[:CIDCountAttempt], uint64(fileSum))
						if err != nil {
							return nil, false, fmt.Errorf("building a root for %s: %e", task, err)
						}
						if int64(len(newRoot)) > blockTarget {
							CIDCountAttempt--
							fileSum -= CIDs[CIDCountAttempt].FileSize
							break
						}
						lastRoot = newRoot
					}

					dagSum += int64(len(lastRoot))

					c, err := r.writePBNode(lastRoot)
					if err != nil {
						return nil, false, fmt.Errorf("writing root for %s: %e", task, err)
					}
					CIDs = CIDs[CIDCountAttempt:]

					cp := &cidSizePair{c, fileSum, dagSum}
					newRoots = append(newRoots, cp)
				}
				CIDs = newRoots
			}
			c = CIDs[0]
		}

		var new bool
		if oldExists {
			new = c.Cid.String() != old.Cid
		} else {
			new = true
		}
		if new {
			r.olds.Cids[task] = &savedCidsPairs{
				Cid:     c.Cid.String(),
				DagSize: c.DagSize,
			}
		}
		return c, new, nil
	}
}

func (r *recursiveTraverser) swap() error {
	<-r.chunkT
	r.tempCarSend, r.tempCarChunk = r.tempCarChunk, r.tempCarSend
	r.sendT <- r.pullBlock()
	err := r.tempCarChunk.Truncate(0)
	if err != nil {
		return err
	}
	r.tempCarOffset = carMaxSize
	return nil
}

type concurrentChunkerManager struct {
	concurrentChunkerCount int64
	t                      chan struct{}
	e                      chan error
}

func (m *concurrentChunkerManager) populate() {
	for i := m.concurrentChunkerCount; i != 0; i-- {
		m.t <- struct{}{}
	}
}

func (m *concurrentChunkerManager) getChunkToken() error {
	select {
	case <-m.t:
		return nil
	case err := <-m.e:
		return m.handleChunkerChanError(err)
	}
}

func (m *concurrentChunkerManager) waitForAllChunks() error {
	select {
	case <-m.t:
		return m.handleChunkerChanError(nil)
	case err := <-m.e:
		return m.handleChunkerChanError(err)
	}
}

func (m *concurrentChunkerManager) handleChunkerChanError(err error) error {
	errs := []error{err}
	for i := m.concurrentChunkerCount - 1; i != 0; i-- {
		select {
		case <-m.t:
		case err = <-m.e:
			errs = append(errs, err)
		}
	}
	return multierr.Combine(errs...)
}

func (r *recursiveTraverser) mkChunk(manager *concurrentChunkerManager, f *os.File, task string, cidR **cidSizePair, varuintHeader []byte, blockHeaderSize, workSize, carOffset, fileOffset int64) {
	err := func() error {
		buff := make([]byte, workSize)
		err := fullReadAt(f, buff, fileOffset)
		if err != nil {
			return fmt.Errorf("reading file: %e", err)
		}
		hash := sha256.Sum256(buff)
		mhash, err := mh.Encode(hash[:], mh.SHA2_256)
		if err != nil {
			return fmt.Errorf("encoding multihash for %s: %e", task, err)
		}
		c := cid.NewCidV1(cid.Raw, mhash)
		*cidR = &cidSizePair{
			Cid:      c,
			FileSize: workSize,
			DagSize:  workSize,
		}

		err = fullWriteAt(r.tempCarChunk, append(varuintHeader, c.Bytes()...), carOffset)
		if err != nil {
			return fmt.Errorf("writing CID + header: %e", err)
		}

		carBlockTarget := carOffset + blockHeaderSize
		err = r.writeToBackBuffer(f, &fileOffset, &carBlockTarget, int(workSize))
		if err != nil {
			return fmt.Errorf("copying \"%s\" to back buffer: %e", task, err)
		}
		return nil
	}()
	if err != nil {
		manager.e <- err
	} else {
		manager.t <- struct{}{}
	}
}

func (r *recursiveTraverser) takeOffset(size int64) (int64, error) {
	if r.tempCarOffset < size {
		err := r.swap()
		if err != nil {
			return 0, fmt.Errorf("failed to pump out: %e", err)
		}
	}
	r.tempCarOffset -= size
	return r.tempCarOffset, nil
}

func (r *recursiveTraverser) mayTakeOffset(size int64) (int64, bool) {
	if r.tempCarOffset < size {
		return 0, true
	}
	r.tempCarOffset -= size
	return r.tempCarOffset, false
}

func (r *recursiveTraverser) writeToBackBuffer(read *os.File, roff *int64, woff *int64, l int) error {
	rsc, err := read.SyscallConn()
	if err != nil {
		return fmt.Errorf("openning SyscallConn of read: %e", err)
	}
	var errr error
	err = rsc.Control(func(rfd uintptr) {
		wsc, err := r.tempCarChunk.SyscallConn()
		if err != nil {
			errr = fmt.Errorf("openning SyscallConn of write: %e", err)
			return
		}
		err = wsc.Control(func(wfd uintptr) {
			for l != 0 {
				n, err := unix.CopyFileRange(int(rfd), roff, int(wfd), woff, l, 0)
				if err != nil {
					if err == io.EOF {
						errr = err
					} else {
						errr = fmt.Errorf("zero-copying to back buffer: %e", err)
					}
					return
				}
				l -= n
			}
		})
		if err != nil {
			errr = fmt.Errorf("getting Control of write: %e", err)
		}
	})
	if err != nil {
		return fmt.Errorf("getting Control of read: %e", err)
	}
	return errr
}

func fullWrite(w io.Writer, buff []byte) error {
	toWrite := len(buff)
	var written int
	for toWrite != written {
		n, err := w.Write(buff[written:])
		if err != nil {
			return err
		}
		written += n
	}
	return nil
}

func fullWriteAt(w io.WriterAt, buff []byte, off int64) error {
	toWrite := int64(len(buff))
	var written int64
	for toWrite != written {
		n, err := w.WriteAt(buff[written:], off+written)
		if err != nil {
			return err
		}
		written += int64(n)
	}
	return nil
}

func fullReadAt(w io.ReaderAt, buff []byte, off int64) error {
	toRead := int64(len(buff))
	var red int64
	for toRead != red {
		n, err := w.ReadAt(buff[red:], off+red)
		if err != nil {
			return err
		}
		red += int64(n)
	}
	return nil
}

func makeFileRoot(ins []*cidSizePair, fileSum uint64) ([]byte, error) {
	links := make([]*pb.PBLink, len(ins))
	sizes := make([]uint64, len(ins))
	for i, v := range ins {
		ds := uint64(v.DagSize)
		links[i] = &pb.PBLink{
			Hash:  v.Cid.Bytes(),
			Tsize: &ds,
		}
		sizes[i] = uint64(v.FileSize)
	}

	typ := pb.UnixfsData_File

	unixfsBlob, err := proto.Marshal(&pb.UnixfsData{
		Filesize:   &fileSum,
		Blocksizes: sizes,
		Type:       &typ,
	})
	if err != nil {
		return nil, err
	}

	return proto.Marshal(&pb.PBNode{
		Links: links,
		Data:  unixfsBlob,
	})
}
