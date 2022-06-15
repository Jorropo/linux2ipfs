package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type incrementalFormat = incrementalFormat1

type savedCidsPairs = savedCidsPairs1

type incrementalFormat1 struct {
	Version    uint                        `json:"version,omitempty"`
	Cids       map[string]*savedCidsPairs1 `json:"cids,omitempty"`
	LastUpdate time.Time                   `json:"lastUpdate,omitempty"` // Compat with v0
}

type savedCidsPairs1 struct {
	Cid        string    `json:"cid"`
	DagSize    int64     `json:"dagSize"`
	LastUpdate time.Time `json:"lastUpdate,omitempty"`
}

func dumpIncremental(path string, data incrementalFormat) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("openning %s: %w", path, err)
	}
	defer f.Close()

	err = json.NewEncoder(f).Encode(data)
	if err != nil {
		return fmt.Errorf("encoding %s: %w", path, err)
	}
	err = f.Close()
	if err != nil {
		return fmt.Errorf("closing %s: %w", path, err)
	}
	return nil
}

func loadIncremental(path string) (incrementalFormat, error) {
	f, err := os.Open(path)
	if err != nil {
		return incrementalFormat{}, fmt.Errorf("openning %s, if you havn't created it do \"echo {} > %s\": %w", path, path, err)
	}
	defer f.Close()

	var data incrementalFormat1
	err = json.NewDecoder(f).Decode(&data)
	if err != nil {
		return incrementalFormat{}, fmt.Errorf("decoding: %w", err)
	}
	err = f.Close()
	if err != nil {
		return incrementalFormat{}, fmt.Errorf("closing: %w", err)
	}

	if data.Cids == nil {
		data.Cids = map[string]*savedCidsPairs{}
	}

	switch data.Version {
	case 0:
		talkLock.Lock()
		fmt.Fprintln(os.Stderr, "v0 incremental, migrating to v1")
		talkLock.Unlock()
		data = incremental0to1(data)
	case 1:
	default:
		return incrementalFormat{}, fmt.Errorf("unkown version %d", data.Version)
	}

	return data, nil
}

func incremental0to1(data incrementalFormat1) incrementalFormat1 {
	data.Version = 1
	t := data.LastUpdate
	for _, c := range data.Cids {
		c.LastUpdate = t
	}
	data.LastUpdate = time.Time{} // reset to 0
	return data
}
