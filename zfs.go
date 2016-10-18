package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sort"
	"strings"
	"time"
)

var driver zfsDriver

const timeFormat = "2006-01-02-15-04-05"

type zfsDriver interface {
	CreateSnapshot(name string, label string) error
	Snapshots(filter string) ([]string, error)
	DeleteSnapshot(name string) error
	SendLastSnapshot(filter string, label string, output io.Writer) error
}

func init() {
	SetDriver(&GoZFS{})
}

// SetDriver sets a specific driver to be used to execute the zfs commands
func SetDriver(d zfsDriver) {
	driver = d
}

// TakeSnapshot takes a snapshot from a dataset by its name with a label that's
// suffixed with the current timestamp in the format `-YYYY-MM-DD-HH-mm`
// The Keep argument defines how many versions of this snapshot should be kept
func TakeSnapshot(name string, label string, keep int, send bool, dir string) error {
	oldSnapshots, err := Snapshots(name)
	if err != nil {
		return err
	}

	lbl := fmt.Sprintf("%s-%s", label, time.Now().Format(timeFormat))
	for _, ss := range oldSnapshots {
		if strings.HasSuffix(ss, lbl) {
			return fmt.Errorf("snapshot %s@%s already exists", name, lbl)
		}
	}

	if keep != 0 {
		cleanup(oldSnapshots, keep)
	}

	err = driver.CreateSnapshot(name, lbl)
	if err != nil {
		return err
	}

	if send {
		nameWithoutSlashes := strings.Replace(name, "/", "-", -1)
		// TODO: move snap extension to some constant
		snapshotFile := fmt.Sprintf("%s-%s.snap", nameWithoutSlashes, lbl)
		f, err := os.Create(path.Join(dir, snapshotFile))
		defer f.Close()
		if err != nil {
			return err
		}

		err = driver.SendLastSnapshot(name, label, f)
		if err != nil {
			os.Remove(f.Name())
			return err
		}
		f.Sync()
	}
	return nil
}

func cleanup(snapshots []string, keep int) {
	if len(snapshots) < keep {
		return
	}

	sort.Strings(snapshots)
	for _, ss := range snapshots[:len(snapshots)-keep+1] {
		if err := DeleteSnapshot(ss); err != nil {
			log.Printf("Cleaning up snapshot %s didn't work: %s\n", ss, err)
		}
	}
}

// Snapshots lists returns all existing zfs snapshots. The filter
// argument is used to select snapshots matching a specific name.
// The empty string can be used to select all snapshots
func Snapshots(filter string) ([]string, error) {
	return driver.Snapshots(filter)
}

// DeleteSnapshot deletes a snapshot by its name
func DeleteSnapshot(name string) error {
	return driver.DeleteSnapshot(name)
}
