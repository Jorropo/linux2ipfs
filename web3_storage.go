package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
)

const (
	envWeb3StorageKeyKey = "WEB3_STORAGE_KEY"
	web3StorageEndpoint  = "https://api.web3.storage"
)

var web3StorageDriverCreator = driverCreator{
	factory:    newWeb3StorageDriver,
	help:       web3StorageHelp,
	maxCarSize: 1024 * 1024 * 98, // 98MiB
}

func web3StorageHelp(out io.Writer) {
	fmt.Fprint(out, `  Environ:
  - `+envWeb3StorageKeyKey+` web3.storage API key

`)
}

func newWeb3StorageDriver(input string) (driver, error) {
	if input != "" {
		return nil, fmt.Errorf("non empty web3.storage argument: %q", input)
	}

	key := os.Getenv(envWeb3StorageKeyKey)

	if key == "" {
		return nil, fmt.Errorf("error empty " + envWeb3StorageKeyKey + " envKey")
	}

	d := &web3StorageDriver{
		key: key,
	}
	return d.send, nil
}

type web3StorageDriver struct {
	key string

	client http.Client
}

func (e *web3StorageDriver) send(headerBuffer []byte, car *os.File, carOffset int64) error {
	_, err := car.Seek(carOffset, io.SeekStart)
	if err != nil {
		return fmt.Errorf("error seeking temp file: %w", err)
	}

	req, err := http.NewRequest("POST", web3StorageEndpoint+"/car", io.MultiReader(bytes.NewReader(headerBuffer), car))
	if err != nil {
		return fmt.Errorf("creating the request failed: %w", err)
	}

	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Content-Type", "application/vnd.ipld.car")
	req.Header.Set("Authorization", "Bearer "+e.key)

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("posting failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("non 200 result code: %d / body: %s", resp.StatusCode, string(b))
	}

	return nil
}
