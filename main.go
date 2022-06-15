package main

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	pb "github.com/Jorropo/linux2ipfs/pb"
	proto "google.golang.org/protobuf/proto"

	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	car "github.com/ipld/go-car"
	mh "github.com/multiformats/go-multihash"

	"go.uber.org/multierr"
	"golang.org/x/sys/unix"
)

const (
	defaultBlockTarget     = 1024 * 1024 * 2 // 2 MiB
	defaultInlineLimit     = 128
	tempFileNamePattern    = ".temp.%s.car"
	defaultIncrementalFile = "old.json"
	defaultUploadTries     = 3
	defaultUploadFailedOut = "failed"
	diskAssumedBlockSize   = 4096
	doJobsBuffer           = 1024 * 16

	// Precomputed values
	rawleafCIDLength = 36
	dagPBCIDLength   = 36

	fakeBlockCIDOverheadLength = rawleafCIDLength
	fakeBlockOverheadLength    = 2 /* First full length header */ + fakeBlockCIDOverheadLength
	fakeBlockMinLength         = 1<<7 /* space to fill up the varuint so we don't run into "varuint non minimal" errors */ + (fakeBlockOverheadLength - fakeBlockCIDOverheadLength)
	fakeBlockMaxValue          = uint(len(precomputedEmptyHashes))

	userAgent = "github.com/Jorropo/linux2ipfs"
)

var talkLock sync.Mutex

// Precomputed value
var directoryData = []byte{0x08, 0x01}

func init() {
	flag.Usage = func() {
		o := flag.CommandLine.Output()
		fmt.Fprint(o, "Usage for: "+os.Args[0]+" <target file path>\n\n")
		fmt.Fprint(o, "Drivers:\n")
		for n, d := range drivers {
			fmt.Fprint(o, "- "+n+":\n")
			d.help(o)
		}
		fmt.Fprint(o, `Positional:
  <target file path> REQUIRED

Flags:
`)
		flag.PrintDefaults()
		fmt.Fprintln(o, "Sample:\n  "+envEstuaryKeyKey+"=EST...ARY "+envEstuaryShuttleKey+"=shuttle-4.estuary.tech "+os.Args[0]+" fileToUpload/")
	}
}

func main() {
	os.Exit(mainRet())
}

var blockTarget int64
var carMaxSize int64
var inlineLimit int64
var uploadTries uint
var uploadFailedOut string
var noPad bool

func mainRet() int {
	var incrementalFile string
	var target string
	var concurrentChunkers int64
	var driverToUse driver
	{
		var driverTarget string
		flag.Int64Var(&blockTarget, "block-target", defaultBlockTarget, "Maximum size of blocks.")
		flag.Int64Var(&carMaxSize, "car-size", 0, "Car reset point, this is mostly how big you want your CARs to be, but it actually is at which point does it stop adding more blocks to it, there is often a 1~128MiB more data to sent (the fakeroots and the header), 0 defaults to the driver default.")
		flag.Int64Var(&inlineLimit, "inline-limit", defaultInlineLimit, "The maximum size at which to attempt to inline blocks.")
		flag.StringVar(&incrementalFile, "incremental-file", defaultIncrementalFile, "Path to the file which stores the old CIDs and old update time.")
		flag.Int64Var(&concurrentChunkers, "concurrent-chunkers", 0, "Number of chunkers to concurrently run, 0 == Num CPUs (note, this only works intra file, the discovery loop is still single threaded).")
		flag.UintVar(&uploadTries, "max-upload-attempt", defaultUploadTries, "Number of time to try to upload the resulting cars.")
		flag.StringVar(&uploadFailedOut, "failed-outs", defaultUploadFailedOut, "Where to move failed upload car files in case an upload failed too many times.")
		flag.BoolVar(&noPad, "no-pad", false, "Doesn't pad the data chunks in the output car to "+strconv.FormatUint(diskAssumedBlockSize, 10)+" bytes, make marginally smaller output cars however likely NOT produce reflinked data.")
		flag.StringVar(&driverTarget, "driver", "", "Driver selector.")
		flag.Parse()

		bad := false

		driversAndOptions := strings.SplitN(driverTarget, "-", 2)
		if len(driversAndOptions) == 1 {
			driversAndOptions = append(driversAndOptions, "")
		}

		if blockTarget < 1024 {
			fmt.Fprintln(os.Stderr, "error block-target should be at least 1024 bytes")
			bad = bad || true
		}
		if blockTarget > 1024*1024*2 {
			fmt.Fprintln(os.Stderr, "error block-target cannot be bigger than 2MiB")
			bad = bad || true
		}

		if driverTarget == "" {
			fmt.Fprintf(os.Stderr, "error no driver specified, you can see potential drivers with: %q\n\tExample: \n\t\t%s\n", os.Args[0]+" -help", os.Args[0]+" -driver car "+strings.Join(os.Args[1:], " "))
			bad = bad || true
		} else {
			var ok bool
			driv, ok := drivers[driversAndOptions[0]]
			if !ok {
				fmt.Fprintf(os.Stderr, "error driver: %q not found\n", driverTarget)
				bad = bad || true
			} else {
				var err error
				driverToUse, err = driv.factory(driversAndOptions[1])
				if err != nil {
					fmt.Fprintln(os.Stderr, "error creating driver: "+err.Error())
					bad = bad || true
				}
			}

			if carMaxSize == 0 {
				carMaxSize = driv.maxCarSize
			} else if carMaxSize > driv.maxCarSize {
				fmt.Fprintln(os.Stderr, "error car-size cannot be bigger than driver's maximum")
				bad = bad || true
			}
			if carMaxSize < blockTarget {
				fmt.Fprintln(os.Stderr, "error car-size cannot be smaller than block-target")
				bad = bad || true
			}
			if !noPad {
				if carMaxSize < diskAssumedBlockSize*2 {
					fmt.Fprintln(os.Stderr, "error car-size cannot be smaller than "+strconv.Itoa(diskAssumedBlockSize*2)+" when padding is enabled")
					bad = bad || true
				}
				if blockTarget > (carMaxSize - diskAssumedBlockSize) {
					fmt.Fprintln(os.Stderr, "error car-size + block-target cannot be bigger than car-size - "+strconv.Itoa(diskAssumedBlockSize)+" when padding is enabled")
					bad = bad || true
				}
			}
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
		if concurrentChunkers < 0 {
			fmt.Fprintln(os.Stderr, "error negative concurrent chunkers")
			bad = bad || true
		}
		if uploadTries == 0 {
			fmt.Fprintln(os.Stderr, "error zero max-upload-attempt")
			bad = bad || true
		}
		if uploadFailedOut == "" {
			fmt.Fprintln(os.Stderr, "error empty failed-outs")
			bad = bad || true
		} else if l := len(uploadFailedOut) - 1; uploadFailedOut[l] == '/' {
			uploadFailedOut = uploadFailedOut[:l]
		}

		if args := flag.Args(); len(args) != 1 {
			fmt.Fprintln(os.Stderr, "error expected one positional <target file path>")
			bad = bad || true
		} else {
			target = args[0]
			if len(target) != 0 {
				l := len(target) - 1
				if target[l] == '/' {
					target = target[:l]
				}
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

	cancel := make(chan struct{})
	{
		var cancelOnce sync.Once
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

		go func() {
			select {
			case v := <-sig:
				talkLock.Lock()
				fmt.Fprintln(os.Stderr, "quiting caught signal: "+v.String())
				talkLock.Unlock()
				cancelOnce.Do(func() { close(cancel) })
			case <-cancel:
			}
		}()
		defer cancelOnce.Do(func() { close(cancel) })
	}

	r := &recursiveTraverser{
		tempCarChunk:           tempCarA,
		tempCarSend:            tempCarB,
		tempCarOffset:          carMaxSize,
		statEntries:            make(chan *doJobs, doJobsBuffer),
		statError:              make(chan error),
		cancel:                 cancel,
		chunkT:                 make(chan struct{}, 1),
		sendT:                  make(chan sendJobs, 1),
		concurrentChunkerCount: concurrentChunkers,
		send:                   driverToUse,
		incrementalFile:        incrementalFile,
		dumpJobs:               make(chan incrementalFormat),
	}
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		r.statWorker(target)
	}()
	go func() {
		defer wg.Done()
		defer close(r.dumpJobs)
		lastDumped := r.sendWorker()
		if !lastDumped {
			r.dumpJobs <- r.olds
		}
	}()
	go func() {
		defer wg.Done()
		r.dumpWorker()
	}()
	defer wg.Wait()
	defer close(r.sendT)
	r.chunkT <- struct{}{}

	r.olds, err = loadIncremental(incrementalFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error loading incremental file: "+err.Error())
		return 1
	}

	c, updated, err := r.do()
	if err != nil {
		if !errors.Is(err, errClosing) {
			fmt.Fprintln(os.Stderr, "error doing: "+err.Error())
		}
		return 1
	}
	// swap if there is data remaining in the back buffer
	if r.tempCarOffset != carMaxSize {
		err = r.swap()
		if err != nil {
			fmt.Fprintln(os.Stderr, "error making last swap: "+err.Error())
			return 1
		}
	}

	fmt.Fprintln(os.Stdout, c.Cid.String())

	if updated {
		fmt.Fprintln(os.Stderr, "updated")
	} else {
		fmt.Fprintln(os.Stderr, "non-updated")
	}

	return 0
}

func (r *recursiveTraverser) statWorker(task string) {
	entry, err := os.Lstat(task)
	if err != nil {
		select {
		case r.statError <- err:
		case <-r.cancel:
			return
		}
	}
	err = r.rStatWorker(task, entry)
	if err != nil {
		select {
		case r.statError <- err:
		case <-r.cancel:
			return
		}
	}
}

func (r *recursiveTraverser) rStatWorker(task string, entry os.FileInfo) error {
	job := &doJobs{
		task:  task,
		entry: entry,
	}

	isDir := entry.IsDir()
	var subThings []os.DirEntry
	if isDir {
		var err error
		subThings, err = os.ReadDir(task)
		if err != nil {
			return fmt.Errorf("ReadDir %s: %w", task, err)
		}
		job.subThings = subThings
	}

	select {
	case r.statEntries <- job:
	case <-r.cancel:
		return errClosing
	}

	if isDir {
		for _, v := range subThings {
			sInfo, err := v.Info()
			if err != nil {
				return fmt.Errorf("getting info of %s/%s: %w", job.task, v.Name(), err)
			}
			err = r.rStatWorker(task+"/"+v.Name(), sInfo)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type driver func(headerBuffer []byte, car *os.File, carOffset int64) error
type driverFactory func(params string) (driver, error)
type driverHelper func(output io.Writer)

type driverCreator struct {
	factory    driverFactory
	help       driverHelper
	maxCarSize int64
}

var drivers = map[string]driverCreator{
	"estuary":      estuaryDriverCreator,
	"car":          carDriverCreator,
	"web3.storage": web3StorageDriverCreator,
}

func (r *recursiveTraverser) dumpWorker() {
	for task := range r.dumpJobs {
		err := dumpIncremental(r.incrementalFile, task)
		if err != nil {
			talkLock.Lock()
			fmt.Fprintln(os.Stderr, "error dumping incremental: "+err.Error())
			talkLock.Unlock()
		}
	}
}

func (r *recursiveTraverser) sendWorker() (lastDumped bool) {
TaskLoop:
	for task := range r.sendT {
		lastDumped = false
		if task.offset == carMaxSize || len(task.roots) == 0 {
			// Empty car do nothing.
			continue
		}
		header, offset, err := r.makeSendPayload(task)
		if err != nil {
			panic(fmt.Errorf("creating payload: %w", err))
		}
		err = r.tempCarSend.Sync()
		if err != nil {
			talkLock.Lock()
			fmt.Fprintln(os.Stderr, "error syncing temp file: "+err.Error())
			talkLock.Unlock()
			continue
		}
		for failed := uint(0); failed != uploadTries; failed++ {
			attemptCount := strconv.FormatUint(uint64(failed+1), 10) + " / " + strconv.FormatUint(uint64(uploadTries), 10)
			err := r.send(header, r.tempCarSend, offset)
			if err != nil {
				talkLock.Lock()
				fmt.Fprintln(os.Stderr, attemptCount+" error sending: "+err.Error())
				talkLock.Unlock()
				continue
			}

			r.chunkT <- struct{}{}
			// only dump if the dump worker is not busy
			select {
			case r.dumpJobs <- task.cids:
				lastDumped = true
			default:
			}

			continue TaskLoop
		}

		// Failed, copy to failedOut
		r.makeFailedOutDir.Do(func() {
			os.Mkdir(uploadFailedOut, 0o775)
		})

		n := atomic.AddUint32(&r.failedOutCounter, 1)
		outName := uploadFailedOut + "/" + strconv.FormatUint(uint64(n), 10) + ".car"
		outF, err := os.OpenFile(outName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			outF.Close()
			os.Remove(outName)
			r.chunkT <- struct{}{}
			talkLock.Lock()
			fmt.Fprintln(os.Stderr, "error creating failed out file "+outName+": "+err.Error())
			talkLock.Unlock()
			continue
		}
		err = fullWrite(outF, header)
		if err != nil {
			outF.Close()
			os.Remove(outName)
			r.chunkT <- struct{}{}
			talkLock.Lock()
			fmt.Fprintln(os.Stderr, "error writing header to failed out file "+outName+": "+err.Error())
			talkLock.Unlock()
			continue
		}
		_, err = r.tempCarSend.Seek(offset, 0)
		if err != nil {
			outF.Close()
			os.Remove(outName)
			r.chunkT <- struct{}{}
			talkLock.Lock()
			fmt.Fprintln(os.Stderr, "error seeking car to failed out file "+outName+": "+err.Error())
			talkLock.Unlock()
			continue
		}
		_, err = outF.ReadFrom(r.tempCarSend)
		outF.Close()
		if err != nil {
			os.Remove(outName)
			r.chunkT <- struct{}{}
			talkLock.Lock()
			fmt.Fprintln(os.Stderr, "error copying buffer to "+outName+": "+err.Error())
			talkLock.Unlock()
			continue
		}

		r.chunkT <- struct{}{}
		// only dump if the dump worker is not busy
		select {
		case r.dumpJobs <- task.cids:
			lastDumped = true
		default:
		}
	}

	return
}

func (r *recursiveTraverser) makeSendPayload(job sendJobs) ([]byte, int64, error) {
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
		lastAttempt := 0
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
				return nil, 0, fmt.Errorf("serialising fake root: %w", err)
			}
			lastAttempt = median

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
			// binarysearch round up when it can't find a perfect match
			// lets round down instead else we are one link too big every time
			if int64(len(blockData)) > blockTarget {
				low--
			}
			// in case we finished by a wrong estimation, fix them by reserialising the correct amount
			// also serialise in case there were only 2 links
			if low != lastAttempt {
				blockData, err = proto.Marshal(&pb.PBNode{
					Links: cidsToLink[:low],
					Data:  directoryData,
				})
				if err != nil {
					return nil, 0, fmt.Errorf("serialising fake root: %w", err)
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
			return nil, 0, fmt.Errorf("encoding multihash: %w", err)
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

	// Writing CAR header
	c, err := cid.Cast(cidsToLink[0].Hash)
	if err != nil {
		return nil, 0, fmt.Errorf("casting CID back from bytes: %w", err)
	}
	headerBuffer, err := cbor.DumpObject(&car.CarHeader{
		Roots:   []cid.Cid{c},
		Version: 1,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("serialising header: %w", err)
	}

	varuintHeader := make([]byte, binary.MaxVarintLen64+uint64(len(headerBuffer))+uint64(len(data)))
	uvarintSize := binary.PutUvarint(varuintHeader, uint64(len(headerBuffer)))
	return append(append(varuintHeader[:uvarintSize], headerBuffer...), data...), job.offset, nil
}

func (r *recursiveTraverser) writePBNode(data []byte) (cid.Cid, bool, error) {
	// Making block header
	varuintHeader := make([]byte, binary.MaxVarintLen64+dagPBCIDLength+len(data))
	uvarintSize := binary.PutUvarint(varuintHeader, uint64(dagPBCIDLength)+uint64(len(data)))
	varuintHeader = varuintHeader[:uvarintSize]

	fullSize := int64(len(data)) + int64(uvarintSize) + int64(rawleafCIDLength)

	h := sha256.Sum256(data)
	mhash, err := mh.Encode(h[:], mh.SHA2_256)
	if err != nil {
		return cid.Cid{}, false, fmt.Errorf("encoding multihash: %w", err)
	}
	fakeLeaf := cid.NewCidV1(cid.Raw, mhash)
	rootBlock := append(append(varuintHeader, fakeLeaf.Bytes()...), data...)
	r.toSend = append(r.toSend, &cidSizePair{
		Cid:      fakeLeaf,
		FileSize: fullSize,
		DagSize:  fullSize,
	})

	off, swapped, err := r.takeOffset(fullSize)
	if err != nil {
		return cid.Cid{}, false, fmt.Errorf("taking offset: %w", err)
	}
	err = fullWriteAt(r.tempCarChunk, rootBlock, off)
	if err != nil {
		return cid.Cid{}, false, fmt.Errorf("writing root's header: %w", err)
	}

	return cid.NewCidV1(cid.DagProtobuf, mhash), swapped, nil
}

type sendJobs struct {
	roots  []*cidSizePair
	offset int64
	cids   incrementalFormat
}

type recursiveTraverser struct {
	tempCarOffset int64
	tempCarChunk  *os.File
	tempCarSend   *os.File

	statEntries chan *doJobs
	statError   chan error
	cancel      chan struct{}

	chunkT   chan struct{}
	sendT    chan sendJobs
	dumpJobs chan incrementalFormat

	send driver

	concurrentChunkerCount int64

	toSend []*cidSizePair

	makeFailedOutDir sync.Once
	failedOutCounter uint32

	olds            incrementalFormat
	incrementalFile string
}

type doJobs struct {
	task      string
	entry     os.FileInfo
	subThings []os.DirEntry
}

func (r *recursiveTraverser) pullBlock() sendJobs {
	curCids := make(map[string]*savedCidsPairs, len(r.olds.Cids))
	for k, v := range r.olds.Cids {
		curCids[k] = v
	}

	j := sendJobs{roots: r.toSend,
		offset: r.tempCarOffset,
		cids: incrementalFormat{
			Version: r.olds.Version,
			Cids:    curCids,
		},
	}
	r.toSend = nil
	return j
}

type cidSizePair struct {
	Cid      cid.Cid
	FileSize int64
	DagSize  int64
}

func (cp *cidSizePair) String() string {
	return cp.Cid.String() + " fileSize: " + strconv.FormatInt(cp.FileSize, 10) + " dagSize: " + strconv.FormatInt(cp.DagSize, 10)
}

func (r *recursiveTraverser) do() (*cidSizePair, bool, error) {
	var job *doJobs
	select {
	case <-r.cancel:
		return nil, false, errClosing
	case job = <-r.statEntries:
	case err := <-r.statError:
		return nil, false, err
	}
	ctime := job.entry.ModTime()
	old, oldExists := r.olds.Cids[job.task]
	switch job.entry.Mode() & os.ModeType {
	case os.ModeSymlink:
		if oldExists && !ctime.After(old.LastUpdate) {
			// Recover old link
			c, err := cid.Decode(old.Cid)
			if err != nil {
				return nil, false, fmt.Errorf("decoding old cid \"%s\": %w", old.Cid, err)
			}
			return &cidSizePair{
				Cid:     c,
				DagSize: old.DagSize,
			}, false, nil
		}

		target, err := os.Readlink(job.task)
		if err != nil {
			return nil, false, fmt.Errorf("resolving symlink %s: %w", job.task, err)
		}

		typ := pb.UnixfsData_Symlink

		data, err := proto.Marshal(&pb.UnixfsData{
			Type: &typ,
			Data: []byte(target),
		})
		if err != nil {
			return nil, false, fmt.Errorf("marshaling unixfs %s: %w\n", job.task, err)
		}

		data, err = proto.Marshal(&pb.PBNode{Data: data})
		if err != nil {
			return nil, false, fmt.Errorf("marshaling ipld %s: %w\n", job.task, err)
		}

		hash, err := mh.Encode(data, mh.IDENTITY)
		if err != nil {
			return nil, false, fmt.Errorf("inlining %s: %w", job.task, err)
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
			r.olds.Cids[job.task] = &savedCidsPairs{
				Cid:        c.String(),
				DagSize:    dagSize,
				LastUpdate: ctime,
			}
		}
		return &cidSizePair{
			Cid:     c,
			DagSize: dagSize,
		}, new, nil

	case os.ModeDir:
		new := !oldExists || ctime.After(old.LastUpdate)

		links := make([]*pb.PBLink, len(job.subThings))

		var dagSum int64
		sCids := make([]*cidSizePair, len(job.subThings))
		for i, v := range job.subThings {
			sCid, updated, err := r.do()
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
				return nil, false, fmt.Errorf("decoding old cid \"%s\": %w", old.Cid, err)
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
			return nil, false, fmt.Errorf("can't Marshal directory %s: %w", job.task, err)
		}
		if int64(len(data)) > blockTarget {
			return nil, false, fmt.Errorf("%s is exceed block limit, TODO: support sharding directories", job.task)
		}

		dagSum += int64(len(data))

		c, _, err := r.writePBNode(data)
		if err != nil {
			return nil, false, fmt.Errorf("writing directory %s: %w", job.task, err)
		}

		if oldExists {
			new = c.String() != old.Cid
		} else {
			new = true
		}
		if new {
			r.olds.Cids[job.task] = &savedCidsPairs{
				Cid:        c.String(),
				DagSize:    dagSum,
				LastUpdate: ctime,
			}
		}
		return &cidSizePair{
			Cid:     c,
			DagSize: dagSum,
		}, new, nil

	default:
		// File
		if oldExists && !ctime.After(old.LastUpdate) {
			// Recover old link
			c, err := cid.Decode(old.Cid)
			if err != nil {
				return nil, false, fmt.Errorf("decoding old cid \"%s\": %w", old.Cid, err)
			}
			return &cidSizePair{
				Cid:     c,
				DagSize: old.DagSize,
			}, false, nil
		}

		f, err := os.Open(job.task)
		if err != nil {
			return nil, false, fmt.Errorf("failed to open %s: %w", job.task, err)
		}
		defer f.Close()

		var c *cidSizePair
		oldOffset := r.tempCarOffset
		size := job.entry.Size()
		// This check is really important and doesn't only deal with inlining
		// This ensures that no zero sized files is chunked (the chunker would fail horribly with thoses)
		if size <= inlineLimit {
			data := make([]byte, size)
			_, err := io.ReadFull(f, data)
			if err != nil {
				return nil, false, fmt.Errorf("reading %s: %w", job.task, err)
			}
			hash, err := mh.Encode(data, mh.IDENTITY)
			if err != nil {
				return nil, false, fmt.Errorf("inlining %s: %w", job.task, err)
			}
			c = &cidSizePair{
				Cid:      cid.NewCidV1(cid.Raw, hash),
				FileSize: size,
				DagSize:  size,
			}

		} else {
			blockCount := (size-1)/blockTarget + 1
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
				workSize := blockTarget
				if remaining := size - (i * blockTarget); remaining < workSize {
					// Last block
					workSize = remaining
				}

				varuintHeader := make([]byte, binary.MaxVarintLen64+rawleafCIDLength)
				uvarintSize := binary.PutUvarint(varuintHeader, uint64(rawleafCIDLength)+uint64(workSize))
				varuintHeader = varuintHeader[:uvarintSize]

				blockHeaderSize := uvarintSize + rawleafCIDLength
				dataSize := int64(blockHeaderSize) + workSize
				fullSize := dataSize

				var toPad uint64
				if !noPad {
					toPad = (uint64(r.tempCarOffset)%diskAssumedBlockSize + diskAssumedBlockSize - uint64(workSize)%diskAssumedBlockSize) % diskAssumedBlockSize
					if toPad != 0 && toPad < fakeBlockMinLength {
						// we can't pad so little, pad to the next size
						toPad += diskAssumedBlockSize
					}
				}
				fullSize += int64(toPad)

				writePadBlock := r.tempCarOffset != carMaxSize // If that true that mean we are writting the first (so end because we write backward) block, so no need to write padding

				carOffset, needSwap := r.mayTakeOffset(fullSize)

				if needSwap {
					err := manager.waitForAllChunks()
					if err != nil {
						return nil, false, err
					}
					r.toSend = append(r.toSend, CIDs[sentCounter:i]...)
					sentCounter = i
					err = r.swap()
					if err != nil {
						return nil, false, fmt.Errorf("swapping: %w", err)
					}
					manager.populate()
					oldOffset = r.tempCarOffset
					if !noPad {
						toPad = (uint64(r.tempCarOffset)%diskAssumedBlockSize + diskAssumedBlockSize - uint64(workSize)%diskAssumedBlockSize) % diskAssumedBlockSize
						if toPad != 0 && toPad < fakeBlockMinLength {
							// we can't pad so little, pad to the next size
							toPad += diskAssumedBlockSize
						}
						fullSize = int64(toPad) + dataSize
					}
					writePadBlock = false
					r.tempCarOffset -= fullSize
					carOffset = r.tempCarOffset
				}

				if !writePadBlock {
					toPad = 0
				}

				err := manager.getChunkToken()
				if err != nil {
					return nil, false, err
				}
				go r.mkChunk(manager, f, job.task, &CIDs[i], varuintHeader, int64(blockHeaderSize), workSize, carOffset, fileOffset, uint16(toPad))
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

					low := 2
					lastAttempt := 0
					high := len(CIDs) - 1
					var lastRoot []byte
					var err error
					var fileSum int64
					for low <= high {
						median := (low + high) / 2

						lastRoot, fileSum, err = makeFileRoot(CIDs[:median])
						if err != nil {
							return nil, false, fmt.Errorf("building a root for %s: %w", job.task, err)
						}
						lastAttempt = median

						l := int64(len(lastRoot))
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
						// binarysearch round up when it can't find a perfect match
						// lets round down instead else we are one link too big every time
						if int64(len(lastRoot)) > blockTarget {
							low--
						}
						// in case we finished by a wrong estimation, fix them by reserialising the correct amount
						// also serialise in case there were only 2 links
						if low != lastAttempt {
							lastRoot, fileSum, err = makeFileRoot(CIDs[:low])
							if err != nil {
								return nil, false, fmt.Errorf("building a root for %s: %w", job.task, err)
							}
						}
					}
				AfterPerfectSize:
					dagSum := int64(len(lastRoot))
					for _, v := range CIDs[:low] {
						dagSum += v.DagSize
					}

					c, swapped, err := r.writePBNode(lastRoot)
					if err != nil {
						return nil, false, fmt.Errorf("writing root for %s: %w", job.task, err)
					}
					if swapped {
						oldOffset = carMaxSize
					}
					CIDs = CIDs[low:]

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
			r.olds.Cids[job.task] = &savedCidsPairs{
				Cid:        c.Cid.String(),
				DagSize:    c.DagSize,
				LastUpdate: ctime,
			}
		} else {
			// Zero (punch actually to free up disk blocks) data we unremove
			sizeToRemove := oldOffset - r.tempCarOffset
			if sizeToRemove != 0 {
				conn, err := r.tempCarChunk.SyscallConn()
				if err != nil {
					return nil, false, fmt.Errorf("getting syscallconn for punching for %s: %w", job.task, err)
				}
				err = conn.Control(func(fd uintptr) {
					err = unix.Fallocate(int(fd), unix.FALLOC_FL_KEEP_SIZE|unix.FALLOC_FL_PUNCH_HOLE, r.tempCarOffset, sizeToRemove)
				})
				if err != nil {
					return nil, false, fmt.Errorf("punching hole in %s (off: %d, size: %d): %w", job.task, r.tempCarOffset, sizeToRemove, err)
				}
				r.tempCarOffset = oldOffset
			}
		}
		return c, new, nil
	}
}

var errClosing = errors.New("shutting down")

func (r *recursiveTraverser) swap() error {
	select {
	case <-r.cancel:
		return errClosing
	case <-r.chunkT:
	}
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

func (r *recursiveTraverser) mkChunk(manager *concurrentChunkerManager, f *os.File, task string, cidR **cidSizePair, varuintHeader []byte, blockHeaderSize, workSize, carOffset, fileOffset int64, toPad uint16) {
	err := func() error {
		h := sha256.New()
		_, err := io.Copy(h, io.NewSectionReader(f, fileOffset, workSize))
		if err != nil {
			return fmt.Errorf("hashing data for %s: %w", task, err)
		}
		mhash, err := mh.Encode(h.Sum(nil), mh.SHA2_256)
		if err != nil {
			return fmt.Errorf("encoding multihash for %s: %w", task, err)
		}
		c := cid.NewCidV1(cid.Raw, mhash)
		*cidR = &cidSizePair{
			Cid:      c,
			FileSize: workSize,
			DagSize:  workSize,
		}

		err = fullWriteAt(r.tempCarChunk, append(varuintHeader, c.Bytes()...), carOffset)
		if err != nil {
			return fmt.Errorf("writing CID + header: %w", err)
		}

		carBlockTarget := carOffset + blockHeaderSize
		err = r.writeToBackBuffer(f, fileOffset, carBlockTarget, int(workSize))
		if err != nil {
			return fmt.Errorf("copying \"%s\" to back buffer: %w", task, err)
		}

		// Padding
		if toPad != 0 {
			if toPad < fakeBlockMinLength || uint(toPad) > fakeBlockMaxValue {
				panic("internal bug!")
			}

			buff := createPadBlockHeader(toPad)

			err = fullWriteAt(r.tempCarChunk, buff, carBlockTarget+workSize)
			if err != nil {
				return fmt.Errorf("writing padding: %w", err)
			}
		}
		return nil
	}()
	if err != nil {
		manager.e <- err
	} else {
		manager.t <- struct{}{}
	}
}

func createPadBlockHeader(toPad uint16) []byte {
	buff := make([]byte, fakeBlockOverheadLength)
	headerBuffer := toPad - (fakeBlockOverheadLength - fakeBlockCIDOverheadLength)
	buff[0] = uint8(headerBuffer&127) | 0x80 // Low byte
	buff[1] = uint8(headerBuffer >> 7)       // High byte
	buff[2] = 1                              // CIDv1
	buff[3] = 0x55                           // Raw multicodec
	buff[4] = 0x12                           // sha256 multihash
	buff[5] = 32                             // Multihash length

	return append(buff[:6], precomputedEmptyHashes[toPad-fakeBlockOverheadLength][:]...)
}

func (r *recursiveTraverser) takeOffset(size int64) (int64, bool, error) {
	swapped := r.tempCarOffset < size
	if swapped {
		err := r.swap()
		if err != nil {
			return 0, false, fmt.Errorf("failed to pump out: %w", err)
		}
	}
	r.tempCarOffset -= size
	return r.tempCarOffset, swapped, nil
}

func (r *recursiveTraverser) mayTakeOffset(size int64) (int64, bool) {
	if r.tempCarOffset < size {
		return 0, true
	}
	r.tempCarOffset -= size
	return r.tempCarOffset, false
}

func (r *recursiveTraverser) writeToBackBuffer(read *os.File, roff int64, woff int64, l int) error {
	rsc, err := read.SyscallConn()
	if err != nil {
		return fmt.Errorf("openning SyscallConn of read: %w", err)
	}
	var errr error
	err = rsc.Control(func(rfd uintptr) {
		wsc, err := r.tempCarChunk.SyscallConn()
		if err != nil {
			errr = fmt.Errorf("openning SyscallConn of write: %w", err)
			return
		}
		err = wsc.Control(func(wfd uintptr) {
			for l != 0 {
				n, err := unix.CopyFileRange(int(rfd), &roff, int(wfd), &woff, l, 0)
				if err != nil {
					if err == io.EOF {
						errr = err
					} else {
						errr = fmt.Errorf("zero-copying to back buffer: %w", err)
					}
					return
				}
				l -= n
			}
		})
		if err != nil {
			errr = fmt.Errorf("getting Control of write: %w", err)
		}
	})
	if err != nil {
		return fmt.Errorf("getting Control of read: %w", err)
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

func makeFileRoot(ins []*cidSizePair) ([]byte, int64, error) {
	links := make([]*pb.PBLink, len(ins))
	sizes := make([]uint64, len(ins))
	var fileSum int64
	for i, v := range ins {
		fileSum += v.FileSize
		ds := uint64(v.DagSize)
		links[i] = &pb.PBLink{
			Hash:  v.Cid.Bytes(),
			Tsize: &ds,
		}
		sizes[i] = uint64(v.FileSize)
	}

	typ := pb.UnixfsData_File

	ufileSum := uint64(fileSum)
	unixfsBlob, err := proto.Marshal(&pb.UnixfsData{
		Filesize:   &ufileSum,
		Blocksizes: sizes,
		Type:       &typ,
	})
	if err != nil {
		return nil, 0, err
	}

	data, err := proto.Marshal(&pb.PBNode{
		Links: links,
		Data:  unixfsBlob,
	})
	return data, fileSum, err
}
