package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"
)

type testBackend struct {
	volumes   []string
	snapshots []string
}

func TestTakeSnapshot(t *testing.T) {
	inboxPath, err := ioutil.TempDir("", "inbox")
	if err != nil {
		panic(err)
	}
	driver := &testBackend{
		volumes:   []string{"foo"},
		snapshots: []string{},
	}
	SetDriver(driver)

	TakeSnapshot("foo", "bar", 0, false, inboxPath)
	t.Error("Not Implemented")
}

func (b *testBackend) CreateSnapshot(name string, label string) error {
	var found bool
	for _, v := range b.volumes {
		if v == name {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("Volume %s not found\n", name)
	}
	b.snapshots = append(b.snapshots, fmt.Sprintf("%s@%s", name, label))
	return nil
}

func (b *testBackend) Snapshots(filter string) ([]string, error) {
	var sn []string
	for _, s := range b.snapshots {
		if strings.Contains(s, filter) {
			sn = append(sn, s)
		}
	}
	return sn, nil
}

func (b *testBackend) DeleteSnapshot(name string) error {
	if err := b.hasSnapshot(name); err != nil {
		return err
	}

	for i, s := range b.snapshots {
		if s == name {
			b.snapshots = append(s.snaphots[:i], s.snaphots[i+1:]...)
			break
		}
	}
	return nil
}

func (b *testBackend) SendSnapshots(from, to string, output io.Writer) error {
	if from != "" {
		if err := b.hasSnapshot(from); err != nil {
			return err
		}
	}
	if err := b.hasSnapshot(to); err != nil {
		return err
	}

	fmt.Fprintf(output, "%s to %s", from, to)
	return nil
}

func (b *testBackend) hasSnapshot(name string) error {
	for _, s := range b.snapshots {
		if s == name {
			return nil
		}
	}
	return fmt.Errorf("snapshot %s not found", name)
}
