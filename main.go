package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"

	pb "github.com/Jorropo/linux2ipfs/pb"
	proto "google.golang.org/protobuf/proto"

	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	car "github.com/ipld/go-car"
	mh "github.com/multiformats/go-multihash"
)

const blockTarget = 1024 * 1024                      // 1 MiB
const carMaxSize = 32*1024*1024*1024 - 1024*1024*128 // ~32 GiB
const inlineLimit = 32
const tempFileNamePattern = ".temp.%s.car"

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
}

func main() {
	os.Exit(mainRet())
}

func mainRet() int {
	tempFileName := fmt.Sprintf(tempFileNamePattern, "A")
	tempCarA, err := os.OpenFile(tempFileName, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error openning tempCar: "+err.Error())
		return 1
	}
	defer os.Remove(tempFileName)
	defer tempCarA.Close()

	tempCarAConn, err := tempCarA.SyscallConn()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error getting SyscallConn for tempCar: "+err.Error())
		return 1
	}

	var controlR int
	err = tempCarAConn.Control(func(tempCarAFd uintptr) {
		controlR = func(tempCarAFd int) int {
			tempFileName := fmt.Sprintf(tempFileNamePattern, "B")
			tempCarB, err := os.OpenFile(tempFileName, os.O_CREATE|os.O_RDWR, 0o600)
			if err != nil {
				fmt.Fprintln(os.Stderr, "error openning tempCar: "+err.Error())
				return 1
			}
			defer os.Remove(tempFileName)
			defer tempCarB.Close()

			tempCarBConn, err := tempCarB.SyscallConn()
			if err != nil {
				fmt.Fprintln(os.Stderr, "error getting SyscallConn for tempCar: "+err.Error())
				return 1
			}

			err = tempCarBConn.Control(func(tempCarBFd uintptr) {
				controlR = func(tempCarBFd int) int {
					r := &recursiveTraverser{
						tempCarChunk:  swapAbleFile{tempCarA, tempCarAFd},
						tempCarSend:   swapAbleFile{tempCarB, tempCarBFd},
						tempCarOffset: carMaxSize,
						chunkT:        make(chan struct{}, 1),
						sendT:         make(chan sendJobs, 1),
						sendOver:      make(chan struct{}),
					}
					r.chunkT <- struct{}{}
					go r.sendWorker()

					target := os.Args[1]
					entry, err := os.Lstat(target)
					if err != nil {
						fmt.Fprintln(os.Stderr, "error stating "+target+": "+err.Error())
						return 1
					}
					c, err := r.do(target, entry)
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

					<-r.sendOver

					return 0
				}(int(tempCarBFd))
			})
			if err != nil {
				fmt.Fprintln(os.Stderr, "error getting FD for tempCarB: "+err.Error())
				return 1
			}

			return controlR
		}(int(tempCarAFd))
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error getting FD for tempCarA: "+err.Error())
		return 1
	}

	return controlR
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
	if job.offset == carMaxSize {
		// Empty car do nothing.
		return nil
	}

	var outSize int64
	out, err := os.OpenFile(fmt.Sprintf(os.Args[2], r.sendCounter), os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("opening out file: %e", err)
	}
	r.sendCounter++

	// Writing CAR header
	{
		headerBuffer, err := cbor.DumpObject(&car.CarHeader{
			Roots:   job.roots,
			Version: 1,
		})
		if err != nil {
			return fmt.Errorf("serialising header: %e", err)
		}

		varuintHeader := make([]byte, binary.MaxVarintLen64)
		uvarintSize := binary.PutUvarint(varuintHeader, uint64(len(headerBuffer)))
		outSize += int64(uvarintSize)
		err = fullWrite(out, varuintHeader[:uvarintSize])
		if err != nil {
			return fmt.Errorf("writing out header varuint: %e", err)
		}

		outSize += int64(len(headerBuffer))
		err = fullWrite(out, headerBuffer)
		if err != nil {
			return fmt.Errorf("writing out header: %e", err)
		}
	}

	// copying tempCar to out
	tempCar := r.tempCarSend.File
	err = tempCar.Sync()
	if err != nil {
		return fmt.Errorf("syncing temp file: %e", err)
	}
	_, err = tempCar.Seek(job.offset, 0)
	if err != nil {
		return fmt.Errorf("seeking temp file: %e", err)
	}
	_, err = io.Copy(out, tempCar)
	if err != nil {
		return fmt.Errorf("copying to out file: %e", err)
	}
	outSize += carMaxSize - job.offset
	err = out.Truncate(outSize)
	if err != nil {
		return fmt.Errorf("truncating out file: %e", err)
	}

	return nil
}

func (r *recursiveTraverser) writePBNode(data []byte) (cid.Cid, error) {
	// Making block header
	varuintHeader := make([]byte, binary.MaxVarintLen64+dagPBCIDLength+len(data))
	uvarintSize := binary.PutUvarint(varuintHeader, uint64(dagPBCIDLength)+uint64(len(data)))
	varuintHeader = varuintHeader[:uvarintSize]

	fullSize := len(data) + uvarintSize + dagPBCIDLength

	h := sha256.Sum256(data)
	mhash, err := mh.Encode(h[:], mh.SHA2_256)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("encoding multihash: %e", err)
	}
	c := cid.NewCidV1(cid.DagProtobuf, mhash)
	rootBlock := append(append(varuintHeader, c.Bytes()...), data...)

	off, err := r.takeOffset(int64(fullSize))
	if err != nil {
		return cid.Cid{}, fmt.Errorf("taking offset: %e", err)
	}
	err = fullWriteAt(r.tempCarChunk.File, rootBlock, off)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("writing root's header: %e", err)
	}

	return c, nil
}

type swapAbleFile struct {
	File *os.File
	Fd   int
}

type sendJobs struct {
	roots  []cid.Cid
	offset int64
}

type recursiveTraverser struct {
	tempCarOffset int64
	tempCarChunk  swapAbleFile
	tempCarSend   swapAbleFile

	chunkT   chan struct{}
	sendT    chan sendJobs
	sendOver chan struct{}

	sendCounter uint

	rootsLen uint
	roots    *cidRootsNode
}

type cidRootsNode struct {
	c    *cidSizePair
	next *cidRootsNode
	prev *cidRootsNode
}

func (rt *recursiveTraverser) addRoot(c *cidSizePair) {
	if c.rootNode != nil {
		panic("Internal bug, added same root object twice!")
	}

	oldFirstNode := rt.roots
	r := &cidRootsNode{
		c:    c,
		next: oldFirstNode,
	}
	c.rootNode = r
	rt.roots = r
	if oldFirstNode != nil {
		oldFirstNode.prev = r
	}
	rt.rootsLen++
}

func (rt *recursiveTraverser) removeRoot(c *cidSizePair) {
	r := c.rootNode
	if r == nil {
		return
	}

	n := r.next
	p := r.prev
	if p == nil {
		rt.roots = n
	} else {
		p.next = n
	}
	if n != nil {
		n.prev = p
	}
	rt.rootsLen--
}

func (r *recursiveTraverser) pullRoots() sendJobs {
	cids := make([]cid.Cid, r.rootsLen)
	var i uint
	cur := r.roots
	for cur != nil {
		o := cur.c
		cids[i] = o.Cid
		o.rootNode = nil
		cur = cur.next
		i++
	}
	r.roots = nil
	r.rootsLen = 0
	return sendJobs{cids, r.tempCarOffset}
}

type cidSizePair struct {
	Cid      cid.Cid
	FileSize int64
	DagSize  int64
	rootNode *cidRootsNode
}

func (r *recursiveTraverser) do(task string, entry os.FileInfo) (*cidSizePair, error) {
	switch entry.Mode() & os.ModeType {
	case os.ModeSymlink:
		target, err := os.Readlink(task)
		if err != nil {
			return nil, fmt.Errorf("resolving symlink %s: %e", task, err)
		}

		typ := pb.UnixfsData_Symlink

		data, err := proto.Marshal(&pb.UnixfsData{
			Type: &typ,
			Data: []byte(target),
		})
		if err != nil {
			return nil, fmt.Errorf("marshaling unixfs %s: %e\n", task, err)
		}

		data, err = proto.Marshal(&pb.PBNode{Data: data})
		if err != nil {
			return nil, fmt.Errorf("marshaling ipld %s: %e\n", task, err)
		}

		hash, err := mh.Encode(data, mh.IDENTITY)
		if err != nil {
			return nil, fmt.Errorf("inlining %s: %e", task, err)
		}

		return &cidSizePair{
			Cid:     cid.NewCidV1(cid.DagProtobuf, hash),
			DagSize: int64(len(data)),
		}, nil
	case os.ModeDir:
		subThings, err := os.ReadDir(task)
		if err != nil {
			return nil, fmt.Errorf("ReadDir %s: %e\n", task, err)
		}

		links := make([]*pb.PBLink, len(subThings))

		var fileSum int64
		var dagSum int64
		sCids := make([]*cidSizePair, len(subThings))
		for i, v := range subThings {
			sInfo, err := v.Info()
			if err != nil {
				return nil, fmt.Errorf("getting info of %s/%s: %e", task, v.Name(), err)
			}
			sCid, err := r.do(task+"/"+v.Name(), sInfo)
			if err != nil {
				return nil, err
			}

			sCids[i] = sCid

			fileSum += sCid.FileSize
			dagSum += sCid.DagSize
			sSize := uint64(sCid.DagSize)
			n := v.Name()
			links[i] = &pb.PBLink{
				Name:  &n,
				Tsize: &sSize,
				Hash:  sCid.Cid.Bytes(),
			}
		}

		data, err := proto.Marshal(&pb.PBNode{
			Links: links,
			Data:  directoryData,
		})
		if err != nil {
			return nil, fmt.Errorf("can't Marshal directory %s: %e", task, err)
		}
		if len(data) > blockTarget {
			return nil, fmt.Errorf("%s is exceed block limit, TODO: support sharding directories", task)
		}

		dagSum += int64(len(data))

		c, err := r.writePBNode(data)
		if err != nil {
			return nil, fmt.Errorf("writing directory %s: %e", task, err)
		}

		for _, v := range sCids {
			r.removeRoot(v)
		}

		cp := &cidSizePair{c, fileSum, dagSum, nil}
		r.addRoot(cp)

		return cp, nil
	default:
		// File
		f, err := os.Open(task)
		if err != nil {
			return nil, fmt.Errorf("failed to open %s: %e", task, err)
		}
		defer f.Close()

		var fileOffset int64

		size := entry.Size()
		var blockCount int64
		if size == 0 {
			blockCount = 1
		} else {
			blockCount = (size-1)/blockTarget + 1
		}
		sizeLeft := size
		CIDs := make([]*cidSizePair, blockCount)

		for i := int64(0); i != blockCount; i++ {
			workSize := sizeLeft
			if workSize > blockTarget {
				workSize = blockTarget
			}
			sizeLeft -= workSize

			if workSize <= inlineLimit {
				data := make([]byte, workSize)
				_, err := io.ReadFull(f, data)
				if err != nil {
					return nil, fmt.Errorf("reading %s: %e", task, err)
				}
				hash, err := mh.Encode(data, mh.IDENTITY)
				if err != nil {
					return nil, fmt.Errorf("inlining %s: %e", task, err)
				}
				CIDs[i] = &cidSizePair{
					Cid:      cid.NewCidV1(cid.Raw, hash),
					FileSize: workSize,
					DagSize:  workSize,
				}
				continue
			}

			varuintHeader := make([]byte, binary.MaxVarintLen64+rawleafCIDLength)
			uvarintSize := binary.PutUvarint(varuintHeader, uint64(rawleafCIDLength)+uint64(workSize))
			varuintHeader = varuintHeader[:uvarintSize]

			blockHeaderSize := uvarintSize + rawleafCIDLength

			carOffset, err := r.takeOffset(int64(blockHeaderSize) + workSize)
			if err != nil {
				return nil, err
			}

			hash := sha256.New()
			_, err = io.CopyN(hash, f, workSize)
			if err != nil {
				return nil, fmt.Errorf("hashing %s: %e", task, err)
			}
			mhash, err := mh.Encode(hash.Sum(nil), mh.SHA2_256)
			if err != nil {
				return nil, fmt.Errorf("encoding multihash for %s: %e", task, err)
			}
			c := cid.NewCidV1(cid.Raw, mhash)
			cp := &cidSizePair{
				Cid:      c,
				FileSize: workSize,
				DagSize:  workSize,
			}
			r.addRoot(cp)
			CIDs[i] = cp

			err = fullWriteAt(r.tempCarChunk.File, append(varuintHeader, c.Bytes()...), carOffset)
			if err != nil {
				return nil, fmt.Errorf("writing CID + header: %e", err)
			}

			fsc, err := f.SyscallConn()
			if err != nil {
				return nil, fmt.Errorf("openning SyscallConn for %s: %e", task, err)
			}
			var errr error
			err = fsc.Control(func(rfd uintptr) {
				// CopyFileRange updates the offset pointers, so let's not clobber them
				carBlockTarget := carOffset + int64(blockHeaderSize)
				_, err := unix.CopyFileRange(int(rfd), &fileOffset, r.tempCarChunk.Fd, &carBlockTarget, int(workSize), 0)
				if err != nil {
					errr = fmt.Errorf("error zero-copying for %s: %e", task, err)
					return
				}
			})
			if err != nil {
				return nil, fmt.Errorf("controling for %s: %e", task, err)
			}
			if errr != nil {
				return nil, errr
			}
		}

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
					return nil, fmt.Errorf("building a root for %s: %e", task, err)
				}
				for len(lastRoot) < blockTarget && len(CIDs) > CIDCountAttempt {
					fileSum += CIDs[CIDCountAttempt].FileSize
					CIDCountAttempt++
					newRoot, err := makeFileRoot(CIDs[:CIDCountAttempt], uint64(fileSum))
					if err != nil {
						return nil, fmt.Errorf("building a root for %s: %e", task, err)
					}
					if len(newRoot) > blockTarget {
						CIDCountAttempt--
						fileSum -= CIDs[CIDCountAttempt].FileSize
						break
					}
					lastRoot = newRoot
				}

				dagSum += int64(len(lastRoot))

				c, err := r.writePBNode(lastRoot)
				if err != nil {
					return nil, fmt.Errorf("writing root for %s: %e", task, err)
				}

				for _, v := range CIDs[:CIDCountAttempt] {
					r.removeRoot(v)
				}
				CIDs = CIDs[CIDCountAttempt:]

				cp := &cidSizePair{c, fileSum, dagSum, nil}
				r.addRoot(cp)
				newRoots = append(newRoots, cp)
			}
			CIDs = newRoots
		}

		return CIDs[0], nil
	}
}

func (r *recursiveTraverser) swap() error {
	<-r.chunkT
	r.tempCarSend, r.tempCarChunk = r.tempCarChunk, r.tempCarSend
	r.sendT <- r.pullRoots()
	err := r.tempCarChunk.File.Truncate(0)
	if err != nil {
		return err
	}
	r.tempCarOffset = carMaxSize
	return nil
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
