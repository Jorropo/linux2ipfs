package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
)

const (
	envEstuaryKeyKey     = "ESTUARY_KEY"
	envEstuaryShuttleKey = "ESTUARY_SHUTTLE"
)

var estuaryDriverCreator = driverCreator{
	factory:    newEstuaryDriver,
	help:       estuaryHelp,
	maxCarSize: 32*1024*1024*1024 - 1024*1024*128, // ~32 GiB
}

func estuaryHelp(out io.Writer) {
	fmt.Fprint(out, `  Environ:
  - `+envEstuaryKeyKey+` estuary API key
  - `+envEstuaryShuttleKey+` shuttle domain

`)
}

func newEstuaryDriver(input string) (driver, error) {
	if input != "" {
		return nil, fmt.Errorf("non empty estuary argument: %q", input)
	}

	key := os.Getenv(envEstuaryKeyKey)
	shuttle := os.Getenv(envEstuaryShuttleKey)

	if key == "" {
		return nil, fmt.Errorf("error empty " + envEstuaryKeyKey + " envKey")
	}
	if shuttle == "" {
		return nil, fmt.Errorf("error empty " + envEstuaryShuttleKey + " envKey")
	}

	d := &estuaryDriver{
		key:     key,
		shuttle: "https://" + shuttle + "/content/add-car",
	}
	return d.send, nil
}

type estuaryDriver struct {
	shuttle string
	key     string

	client http.Client
}

func (e *estuaryDriver) send(headerBuffer []byte, car *os.File, carOffset int64) error {
	_, err := car.Seek(carOffset, 0)
	if err != nil {
		return fmt.Errorf("error seeking temp file: %e", err)
	}

	req, err := http.NewRequest("POST", e.shuttle, io.MultiReader(bytes.NewReader(headerBuffer), car))
	if err != nil {
		return fmt.Errorf("creating the request failed: %e", err)
	}

	req.Header.Set("Content-Type", "application/car")
	req.Header.Set("Authorization", "Bearer "+e.key)

	resp, err := e.client.Do(req)
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