package main

import (
	"fmt"
	"io"
	"os"
	"sync/atomic"
)

var carDriverCreator = driverCreator{
	factory:    newCarDriver,
	help:       carHelp,
	maxCarSize: 1 << 42,
}

func carHelp(out io.Writer) {
	fmt.Fprint(out, `  Positional:
  - <PATH_FORMAT> defaults to "out.%d.car"

`)
}

func newCarDriver(input string) (driver, error) {
	if input == "" {
		input = "out.%d.car"
	}

	d := &carDriver{
		pathFormat: input,
	}
	return d.send, nil
}

type carDriver struct {
	pathFormat string
	counter    uint32
}

func (c *carDriver) send(headerBuffer []byte, car *os.File, carOffset int64) error {
	_, err := car.Seek(carOffset, 0)
	if err != nil {
		return fmt.Errorf("seeking temp file: %w", err)
	}

	n := atomic.AddUint32(&c.counter, 1)
	outName := fmt.Sprintf(c.pathFormat, n)
	outF, err := os.OpenFile(outName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		outF.Close()
		os.Remove(outName)
		return fmt.Errorf("creating failed out file %q: %w", outName, err)
	}
	headerLen := int64(len(headerBuffer))
	err = fullWrite(outF, headerBuffer)
	if err != nil {
		outF.Close()
		os.Remove(outName)
		return fmt.Errorf("writing header to failed out file %q: %w", outName, err)
	}
	_, err = car.Seek(carOffset, 0)
	if err != nil {
		outF.Close()
		os.Remove(outName)
		return fmt.Errorf("seeking car to failed out file %q: %w", outName, err)
	}

	if !noPad {
		padCar := diskAssumedBlockSize - uint16(carOffset)%diskAssumedBlockSize
		padHeader := (diskAssumedBlockSize - padCar - uint16(headerLen)) % diskAssumedBlockSize

		if padHeader != 0 {
			if padHeader < fakeBlockMinLength {
				// we can't pad so little, pad to the next size
				padHeader += diskAssumedBlockSize
			}

			err = fullWrite(outF, createPadBlockHeader(padHeader))
			if err != nil {
				return fmt.Errorf("writing pad block to %q: %w", outName, err)
			}

			_, err = outF.Seek(headerLen+int64(padHeader), 0)
			if err != nil {
				return fmt.Errorf("seeking after pad block to %q: %w", outName, err)
			}
		}

		if padCar != 0 {
			// Read only so little bytes to continue reading later alligned to the diskAssumedBlockSize
			_, err = outF.ReadFrom(&io.LimitedReader{
				R: car,
				N: int64(padCar),
			})
			if err != nil {
				return fmt.Errorf("precopying the pad car to %q: %w", outName, err)
			}
		}
	}

	_, err = outF.ReadFrom(car)
	outF.Close()
	if err != nil {
		os.Remove(outName)
		return fmt.Errorf("copying buffer to %q: %w", outName, err)
	}

	return nil
}
