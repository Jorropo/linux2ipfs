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

const blockTarget = 1024 * 1024            // 1 MiB
const carMaxSize = 32 * 1024 * 1024 * 1024 // 32 GiB
const inlineLimit = 40
const tempFileName = ".temp.car"

var rawleafCIDLength int
var dagPBCIDLength int

func init() {
	h, err := mh.Encode(make([]byte, 32), mh.SHA2_256)
	if err != nil {
		panic(err)
	}
	rawleafCIDLength = len(cid.NewCidV1(cid.Raw, h).Bytes())
	dagPBCIDLength = len(cid.NewCidV1(cid.DagProtobuf, h).Bytes())
}

func main() {
	os.Exit(mainRet())
}

func mainRet() int {
	tempCar, err := os.OpenFile(tempFileName, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		fmt.Println("error openning tempCar: " + err.Error())
		return 1
	}
	//defer os.Remove(tempFileName)
	defer tempCar.Close()

	tempCarConn, err := tempCar.SyscallConn()
	if err != nil {
		fmt.Println("error getting SyscallConn for tempCar: " + err.Error())
		return 1
	}

	var controlR int
	err = tempCarConn.Control(func(tempCarFd uintptr) {
		controlR = func(tempCarFd int) int {
			r := &recursiveTraverser{
				tempCarFd:     tempCarFd,
				tempCarOffset: carMaxSize,
				tempCarFile:   tempCar,
			}

			c, err := r.do(os.Args[1])
			if err != nil {
				fmt.Println("error doing: " + err.Error())
				return 1
			}
			var outSize int64
			out, err := os.OpenFile(os.Args[2], os.O_CREATE|os.O_WRONLY, 0o600)
			if err != nil {
				fmt.Println("error opening out file: " + err.Error())
				return 1
			}

			// Writing CAR header
			{
				headerBuffer, err := cbor.DumpObject(&car.CarHeader{
					Roots:   []cid.Cid{c.Cid},
					Version: 1,
				})
				if err != nil {
					fmt.Println("error serialising header: " + err.Error())
					return 1
				}

				varuintHeader := make([]byte, binary.MaxVarintLen64)
				uvarintSize := binary.PutUvarint(varuintHeader, uint64(len(headerBuffer)))
				outSize += int64(uvarintSize)
				err = fullWrite(out, varuintHeader[:uvarintSize])
				if err != nil {
					fmt.Println("error writing out header varuint: " + err.Error())
					return 1
				}

				fmt.Printf("cbor length: %d\n", len(headerBuffer))

				outSize += int64(len(headerBuffer))
				err = fullWrite(out, headerBuffer)
				if err != nil {
					fmt.Println("error writing out header: " + err.Error())
					return 1
				}
			}

			// copying tempCar to out
			err = tempCar.Sync()
			if err != nil {
				fmt.Println("error syncing temp file: " + err.Error())
				return 1
			}
			_, err = tempCar.Seek(r.tempCarOffset, 0)
			if err != nil {
				fmt.Println("error seeking temp file: " + err.Error())
				return 1
			}
			_, err = io.Copy(out, tempCar)
			if err != nil {
				fmt.Println("error copying to out file: " + err.Error())
				return 1
			}
			outSize += carMaxSize - r.tempCarOffset
			err = out.Truncate(outSize)
			if err != nil {
				fmt.Println("error truncating out file: " + err.Error())
				return 1
			}

			fmt.Println(c.Cid.String())

			return 0
		}(int(tempCarFd))
	})
	if err != nil {
		fmt.Println("error getting FD for tempCar: " + err.Error())
		return 1
	}

	return controlR
}

type recursiveTraverser struct {
	tempCarOffset int64
	tempCarFile   *os.File
	tempCarFd     int
}

type cidSizePair struct {
	Cid  cid.Cid
	Size int64
}

func (r *recursiveTraverser) do(task string) (cidSizePair, error) {
	info, err := os.Lstat(task)
	if err != nil {
		return cidSizePair{}, fmt.Errorf("error stating %s: %e\n", task, err)
	}
	switch info.Mode() & os.ModeType {
	case os.ModeDir:
		panic("TODO")
	case os.ModeSymlink:
		panic("TODO")
	default:
		// File
		f, err := os.Open(task)
		if err != nil {
			return cidSizePair{}, fmt.Errorf("failed to open %s: %e", task, err)
		}
		defer f.Close()

		var fileOffset int64

		size := info.Size()
		var blockCount int64
		if size == 0 {
			blockCount = 1
		} else {
			blockCount = (size-1)/blockTarget + 1
		}
		sizeLeft := size
		CIDs := make([]cidSizePair, blockCount)

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
					return cidSizePair{}, fmt.Errorf("error reading %s: %e", task, err)
				}
				hash, err := mh.Encode(data, mh.IDENTITY)
				if err != nil {
					return cidSizePair{}, fmt.Errorf("error inlining %s: %e", task, err)
				}
				CIDs[i].Cid = cid.NewCidV1(cid.Raw, hash)
				CIDs[i].Size = workSize
				continue
			}

			varuintHeader := make([]byte, binary.MaxVarintLen64+rawleafCIDLength)
			uvarintSize := binary.PutUvarint(varuintHeader, uint64(rawleafCIDLength)+uint64(workSize))
			varuintHeader = varuintHeader[:uvarintSize]

			blockHeaderSize := uvarintSize + rawleafCIDLength

			carOffset, err := r.takeOffset(int64(blockHeaderSize) + workSize)
			if err != nil {
				return cidSizePair{}, err
			}

			hash := sha256.New()
			_, err = io.CopyN(hash, f, workSize)
			if err != nil {
				return cidSizePair{}, fmt.Errorf("error hashing %s: %e", task, err)
			}
			mhash, err := mh.Encode(hash.Sum(nil), mh.SHA2_256)
			if err != nil {
				return cidSizePair{}, fmt.Errorf("error encoding multihash for %s: %e", task, err)
			}
			c := cid.NewCidV1(cid.Raw, mhash)
			CIDs[i].Cid = c
			CIDs[i].Size = workSize

			err = fullWriteAt(r.tempCarFile, append(varuintHeader, c.Bytes()...), carOffset)
			if err != nil {
				return cidSizePair{}, fmt.Errorf("error writing CID + header: %e", err)
			}

			fsc, err := f.SyscallConn()
			if err != nil {
				return cidSizePair{}, fmt.Errorf("error openning SyscallConn for %s: %e", task, err)
			}
			var errr error
			err = fsc.Control(func(rfd uintptr) {
				// CopyFileRange updates the offset pointers, so let's not clobber them
				carBlockTarget := carOffset + int64(blockHeaderSize)
				tempFileOffset := fileOffset
				_, err := unix.CopyFileRange(int(rfd), &tempFileOffset, r.tempCarFd, &carBlockTarget, int(workSize), 0)
				if err != nil {
					errr = fmt.Errorf("error zero-copying for %s: %e", task, err)
					return
				}
				fileOffset += workSize
			})
			if err != nil {
				return cidSizePair{}, fmt.Errorf("error controling for %s: %e", task, err)
			}
			if errr != nil {
				return cidSizePair{}, errr
			}
		}

		if len(CIDs) == 0 {
			panic("Internal bug!")
		}

		for _, v := range CIDs {
			fmt.Println(v.Cid.String())
		}

		for len(CIDs) != 1 {
			// Generate roots
			var newRoots []cidSizePair
			for len(CIDs) != 0 {
				if len(CIDs) == 1 {
					// Don't create roots that links to one block, just forward that block
					newRoots = append(newRoots, CIDs...)
					break
				}

				var CIDCountAttempt int = 2
				var size int64 = CIDs[0].Size + CIDs[1].Size
				lastRoot, err := makeFileRoot(CIDs[:CIDCountAttempt])
				if err != nil {
					return cidSizePair{}, fmt.Errorf("error building a root for %s: %e", task, err)
				}
				for len(lastRoot) < blockTarget && len(CIDs) > CIDCountAttempt {
					size += CIDs[CIDCountAttempt].Size
					CIDCountAttempt++
					newRoot, err := makeFileRoot(CIDs[:CIDCountAttempt])
					if err != nil {
						return cidSizePair{}, fmt.Errorf("error building a root for %s: %e", task, err)
					}
					if len(newRoot) > blockTarget {
						CIDCountAttempt--
						size -= CIDs[CIDCountAttempt].Size
						break
					}
					lastRoot = newRoot
				}
				CIDs = CIDs[CIDCountAttempt:]

				// Making block header
				varuintHeader := make([]byte, binary.MaxVarintLen64+dagPBCIDLength+len(lastRoot))
				uvarintSize := binary.PutUvarint(varuintHeader, uint64(dagPBCIDLength)+uint64(len(lastRoot)))
				varuintHeader = varuintHeader[:uvarintSize]

				fullSize := len(lastRoot) + uvarintSize + dagPBCIDLength

				h := sha256.Sum256(lastRoot)
				mhash, err := mh.Encode(h[:], mh.SHA2_256)
				if err != nil {
					return cidSizePair{}, fmt.Errorf("error encoding multihash for %s root: %e", task, err)
				}
				c := cid.NewCidV1(cid.DagProtobuf, mhash)
				rootBlock := append(append(varuintHeader, c.Bytes()...), lastRoot...)
				newRoots = append(newRoots, cidSizePair{c, size})

				off, err := r.takeOffset(int64(fullSize))
				if err != nil {
					return cidSizePair{}, fmt.Errorf("error taking offset for %s root: %e", task, err)
				}
				err = fullWriteAt(r.tempCarFile, rootBlock, off)
				if err != nil {
					return cidSizePair{}, fmt.Errorf("error writing %s root's header: %e", task, err)
				}
			}
			CIDs = newRoots
		}

		return CIDs[0], nil
	}
}

func (r *recursiveTraverser) takeOffset(size int64) (int64, error) {
	if r.tempCarOffset < size {
		return 0, fmt.Errorf("Asked for %d bytes while %d are available.", size, r.tempCarOffset)
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

func makeFileRoot(ins []cidSizePair) ([]byte, error) {
	links := make([]*pb.PBLink, len(ins))
	sizes := make([]uint64, len(ins))
	var sum uint64
	for i, v := range ins {
		s := uint64(v.Size)
		links[i] = &pb.PBLink{
			Hash:  v.Cid.Bytes(),
			Tsize: &s,
		}
		sizes[i] = s
		sum += s
	}

	typ := pb.UnixfsData_File

	unixfs := pb.UnixfsData{
		Filesize:   &sum,
		Blocksizes: sizes,
		Type:       &typ,
	}

	unixfsBlob, err := proto.Marshal(&unixfs)
	if err != nil {
		return nil, err
	}

	node := pb.PBNode{
		Links: links,
		Data:  unixfsBlob,
	}

	return proto.Marshal(&node)
}
