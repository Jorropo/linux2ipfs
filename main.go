package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"

	cid "github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

const blockTarget = 1024 * 1024 // 1 Mib
const carMaxSize = 1024 * 1024 * 1024 * 32
const inlineLimit = 40
const tempFileName = "out.car"

var rawleafCIDLength int
var dagCborCIDLength int

func init() {
	h, err := mh.Encode(make([]byte, 32), mh.SHA2_256)
	if err != nil {
		panic(err)
	}
	rawleafCIDLength = len(cid.NewCidV1(cid.Raw, h).Bytes())
	dagCborCIDLength = len(cid.NewCidV1(cid.DagCBOR, h).Bytes())
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
				tempCarFd:   tempCarFd,
				fileOffset:  carMaxSize,
				tempCarFile: tempCar,
			}

			c, err := r.do(os.Args[1])
			if err != nil {
				fmt.Println(err.Error())
				return 1
			}
			fmt.Println(c.String())

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
	fileOffset  int64
	tempCarFile *os.File
	tempCarFd   int
}

func (r *recursiveTraverser) do(task string) (cid.Cid, error) {
	info, err := os.Lstat(task)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("error stating %s: %e\n", task, err)
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
			return cid.Cid{}, fmt.Errorf("failed to open %s: %e", task, err)
		}
		defer f.Close()

		var fileOffset int64

		size := info.Size()
		i := (size-1)/blockTarget + 1
		leavesCIDs := make([]cid.Cid, i)
		if i != 1 {
			panic("TODO: Support multiple blocks files")
		}
		sizeLeft := size

		for i != 0 {
			i--
			workSize := sizeLeft
			if workSize > blockTarget {
				workSize = blockTarget
			}
			sizeLeft -= workSize

			if workSize <= inlineLimit {
				data := make([]byte, workSize)
				_, err := io.ReadFull(f, data)
				if err != nil {
					return cid.Cid{}, fmt.Errorf("error reading %s: %e", task, err)
				}
				hash, err := mh.Encode(data, mh.IDENTITY)
				if err != nil {
					return cid.Cid{}, fmt.Errorf("error inlining %s: %e", task, err)
				}
				leavesCIDs[i] = cid.NewCidV1(cid.Raw, hash)
				continue
			}

			varuintHeader := make([]byte, binary.MaxVarintLen64)
			uvarintSize := binary.PutUvarint(varuintHeader, uint64(workSize))
			varuintHeader = varuintHeader[:uvarintSize]

			blockHeaderSize := uvarintSize + rawleafCIDLength

			carOffset, err := r.takeOffset(int64(blockHeaderSize) + workSize)
			if err != nil {
				return cid.Cid{}, err
			}

			hash := sha256.New()
			_, err = io.CopyN(hash, f, workSize)
			if err != nil {
				return cid.Cid{}, fmt.Errorf("error hashing %s: %e", task, err)
			}
			mhash, err := mh.Encode(hash.Sum(nil), mh.SHA2_256)
			if err != nil {
				return cid.Cid{}, fmt.Errorf("error encoding multihash for %s: %e", task, err)
			}
			c := cid.NewCidV1(cid.Raw, mhash)

			fsc, err := f.SyscallConn()
			if err != nil {
				return cid.Cid{}, fmt.Errorf("error openning SyscallConn for %s: %e", task, err)
			}
			var errr error
			err = fsc.Control(func(rfd uintptr) {
				carBlockTarget := carOffset + int64(blockHeaderSize)
				_, err := unix.CopyFileRange(int(rfd), &fileOffset, int(r.tempCarFd), &carBlockTarget, int(workSize), 0)
				if err != nil {
					errr = fmt.Errorf("error zero-copying for %s: %e", task, err)
					return
				}
				fileOffset += workSize
			})
			if err != nil {
				return cid.Cid{}, fmt.Errorf("error controling for %s: %e", task, err)
			}
			if errr != nil {
				return cid.Cid{}, errr
			}
			leavesCIDs[i] = c
		}

		if len(leavesCIDs) == 1 {
			return leavesCIDs[0], nil
		}

		panic("TODO: support linked many blocks of one file")
	}
}

func (r *recursiveTraverser) takeOffset(size int64) (int64, error) {
	if r.fileOffset < size {
		return 0, fmt.Errorf("Asked for %d bytes while %d are available.", size, r.fileOffset)
	}
	return r.fileOffset - size, nil
}
