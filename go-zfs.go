package main

import (
	"fmt"
	"io"
	"log"
	"os/exec"

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

	if err := assertIsSnapshot(ds); err != nil {
		return err
	}

	return ds.Destroy(zfs.DestroyDefault)
}

// SendSnapshots sends two snapshots incrementally to an io.Writer
// if the from snapshot is an empty string it just sends the to snapshot non incrementally
func (z GoZFS) SendSnapshots(from, to string, output io.Writer) error {
	if from == "" {
		return sendSnapshot(to, output)
	}

	log.Println("zfs", "send", "-i", from, to)
	cmd := exec.Command("zfs", "send", "-i", from, to)
	cmd.Stdout = output
	return cmd.Run()
}

func assertIsSnapshot(ds *zfs.Dataset) error {
	if ds.Type != zfs.DatasetSnapshot {
		return fmt.Errorf("Dataset %s is not a snapshot (it's a %s)", ds.Name, ds.Type)
	}
	return nil
}

func sendSnapshot(name string, output io.Writer) error {
	ds, err := zfs.GetDataset(name)
	if err != nil {
		return err
	}

	if err := assertIsSnapshot(ds); err != nil {
		return err
	}

	return ds.SendSnapshot(output)
}
