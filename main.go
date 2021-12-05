package main

import (
	"fmt"
	"io"
	"os"

	cid "github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

const blockTarget = 1024 * 1024 // 1 Mib
const carMaxSize = 1024 * 1024 * 1024 * 32
const inlineLimit = 40
const tempFileName = ".temp.car"

//import "golang.org/x/sys/unix"

func main() {
	os.Exit(mainRet())
}

func mainRet() int {
	tempCar, err := os.OpenFile(tempFileName, os.O_CREATE|os.O_RDWR, 002)
	if err != nil {
		fmt.Println("error openning tempCar: " + err.Error())
		return 1
	}
	defer os.Remove(tempFileName)
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
				tempCarFd:  tempCarFd,
				fileOffset: carMaxSize,
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
	tempCarFd  int
	fileOffset int64
}

func (r *recursiveTraverser) do(task string) (cid.Cid, error) {
	info, err := os.Lstat(task)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("Error stating %s: %e\n", task, err)
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
			return cid.Cid{}, fmt.Errorf("Failed to open %s: %e", task, err)
		}
		defer f.Close()

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
				var red int
				for red < int(workSize) {
					n, err := f.Read(data[red:])
					red += n
					if err != nil {
						if err == io.EOF {
							break
						}
						return cid.Cid{}, fmt.Errorf("Error reading %s: %e", task, err)
					}
					if red != int(workSize) {
						return cid.Cid{}, fmt.Errorf("inconsistent file size for %s", task)
					}
				}
				hash, err := mh.Encode(data, mh.IDENTITY)
				if err != nil {
					return cid.Cid{}, fmt.Errorf("Error inlining %s: %e", task, err)
				}
				leavesCIDs[i] = cid.NewCidV1(cid.Raw, hash)
				continue
			}

			err := r.ask(workSize)
			if err != nil {
				return cid.Cid{}, err
			}

			panic("TODO: support writing raw leaves")
		}

		if len(leavesCIDs) == 1 {
			return leavesCIDs[0], nil
		}

		panic("TODO: Support linked many blocks of one file")
	}
}

func (r *recursiveTraverser) ask(size int64) error {
	if r.fileOffset < size {
		return fmt.Errorf("Asked for %d bytes while %d are available.", size, r.fileOffset)
	}
	return nil
}
