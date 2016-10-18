package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os/exec"
	"sort"
	"strings"

	"github.com/mistifyio/go-zfs"
)

// GoZFS is a marker struct for the go-zfs backed driver
// go-zfs just calls the zfs executables directly, so it has no
// shared library requirements
type GoZFS struct{}

type datasetSlice []*zfs.Dataset

func (slice datasetSlice) Len() int {
	return len(slice)
}

func (slice datasetSlice) Less(i, j int) bool {
	return slice[i].Name < slice[j].Name
}

func (slice datasetSlice) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// Snapshots returns all snapshots that match a filter string
// the empty filter string returns all existing snapshots
func (z GoZFS) Snapshots(filter string) ([]string, error) {
	var snapshots []string

	datasets, err := zfs.Snapshots(filter)
	if err != nil {
		return nil, err
	}

	for _, ds := range datasets {
		snapshots = append(snapshots, ds.Name)
	}

	return snapshots, nil
}

// CreateSnapshot creates a snapshot with a provided name and label
func (z GoZFS) CreateSnapshot(name string, label string) error {
	ds, err := zfs.GetDataset(name)
	if err != nil {
		return err
	}

	ss, err := ds.Snapshot(label, false)
	if err != nil {
		return err
	}
	log.Printf("created snapshot %s\n", ss.Name)
	return nil
}

// DeleteSnapshot deletes a snapshot by its full name (including the label)
func (z GoZFS) DeleteSnapshot(name string) error {
	ds, err := zfs.GetDataset(name)
	if err != nil {
		return err
	}

	if ds.Type != zfs.DatasetSnapshot {
		return fmt.Errorf("Dataset %s is not a snapshot (it's a %s)", ds.Name, ds.Type)
	}

	return ds.Destroy(zfs.DestroyDefault)
}

// SendLastSnapshot incrementally sends the last snapshot matching a filter string and a label prefix
func (z GoZFS) SendLastSnapshot(filter string, label string, output io.Writer) error {
	var snaps []*zfs.Dataset
	ss, err := zfs.Snapshots(filter)
	if err != nil {
		return err
	}

	nameWithLabel := fmt.Sprintf("%s@%s-", filter, label)
	for _, s := range ss {
		if strings.HasPrefix(s.Name, nameWithLabel) {
			snaps = append(snaps, s)
		}
	}

	sort.Sort(datasetSlice(snaps))
	switch len(snaps) {
	case 0:
		return errors.New("No snapshots found")
	case 1:
		return snaps[0].SendSnapshot(output)
	}

	fmt.Println("zfs", "send", "-i", snaps[len(snaps)-2].Name, snaps[len(snaps)-1].Name)
	cmd := exec.Command("zfs", "send", "-i", snaps[len(snaps)-2].Name, snaps[len(snaps)-1].Name)
	cmd.Stdout = output
	return cmd.Run()
}
