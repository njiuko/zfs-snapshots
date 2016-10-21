package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sort"
	"strings"
	"time"
)

var driver ZFSDriver

const timeFormat = "2006-01-02-15-04-05"

// The ZFSDriver interface describes a type that can be used to interact with the ZFS file system
type ZFSDriver interface {
	CreateSnapshot(name string, label string) error
	Snapshots(filter string) ([]string, error)
	DeleteSnapshot(name string) error
	SendSnapshots(from, to string, output io.Writer) error
}

func init() {
	SetDriver(&GoZFS{})
}

// SetDriver sets a specific driver to be used to execute the zfs commands
func SetDriver(d ZFSDriver) {
	driver = d
}

// TakeSnapshot takes a snapshot from a dataset by its name with a label that's
// suffixed with the current timestamp in the format `-YYYY-MM-DD-HH-mm`
// The Keep argument defines how many versions of this snapshot should be kept. If keep
// is 0, all versions are kept
func TakeSnapshot(name string, label string, keep int, send bool, dir string) error {
	oldSnapshots, err := Snapshots(name)
	if err != nil {
		return err
	}

	labelWithTimestamp := fmt.Sprintf("%s-%s", label, time.Now().Format(timeFormat))
	for _, ss := range oldSnapshots {
		if strings.HasSuffix(ss, labelWithTimestamp) {
			return fmt.Errorf("snapshot %s@%s already exists", name, labelWithTimestamp)
		}
	}

	if keep != 0 {
		cleanup(oldSnapshots, keep)
	}

	err = driver.CreateSnapshot(name, labelWithTimestamp)
	if err != nil {
		return err
	}

	if send {
		nameWithoutSlashes := strings.Replace(name, "/", "-", -1)
		// TODO: move snap extension to some constant
		snapshotFile := fmt.Sprintf("%s-%s.snap", nameWithoutSlashes, labelWithTimestamp)
		f, err := os.Create(path.Join(dir, snapshotFile))
		if err != nil {
			return err
		}
		defer f.Close()

		snapshots, err := driver.Snapshots(name)
		if err != nil {
			return err
		}

		from, to, err := newest(snapshots, name, label)
		if err != nil {
			return err
		}

		err = driver.SendSnapshots(from, to, f)
		if err != nil {
			os.Remove(f.Name())
			return err
		}
		f.Sync()
	}
	return nil
}

func newest(snapshots []string, name, label string) (from string, to string, err error) {
	var filtered []string
	prefix := fmt.Sprintf("%s@%s-", name, label)
	for _, s := range snapshots {
		if strings.HasPrefix(s, prefix) {
			filtered = append(filtered, s)
		}
	}

	sort.Strings(filtered)
	switch len(filtered) {
	case 0:
		err = errors.New("No snapshots found to send")
	case 1:
		to = filtered[0]
	default:
		from = filtered[len(filtered)-2]
		to = filtered[len(filtered)-1]
	}
	return from, to, err
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
