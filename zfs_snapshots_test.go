package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
)

type testBackend struct {
	volumes   []string
	snapshots []string
}

func (b *testBackend) reset(files []string) {
	b.snapshots = []string{}
	for _, f := range files {
		os.Remove(f)
	}
}

func TestTakeSnapshot(t *testing.T) {
	inboxPath, err := ioutil.TempDir("", "inbox")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(inboxPath)

	driver := &testBackend{
		volumes:   []string{"foo"},
		snapshots: []string{},
	}
	SetDriver(driver)

	sendTests := []struct {
		send  bool
		count int
	}{
		{true, 1},
		{false, 0},
	}

	for _, i := range sendTests {
		err = TakeSnapshot("foo", "bar", 0, i.send, inboxPath)
		if err != nil {
			t.Fatal(err)
		}

		files, err := filepath.Glob(path.Join(inboxPath, "*.snap"))
		if err != nil {
			t.Fatal(err)
		}

		if len(files) != i.count {
			t.Errorf("should have been %d snap files, but are %d %#v", i.count, len(files), files)
		}
		driver.reset(files)
	}

	err = TakeSnapshot("foo", "bar", 0, true, inboxPath)
	if err != nil {
		t.Fatal(err)
	}

	files, _ := filepath.Glob(path.Join(inboxPath, "*.snap"))
	out, _ := ioutil.ReadFile(files[0])
	if string(out) != "testsnapshot" {
		t.Errorf("TakeSnapshot didn't write the drivers output to the snapshot file")
	}

	err = TakeSnapshot("doesntexist", "bar", 0, false, inboxPath)
	if err == nil {
		t.Error("TakeSnapshot didn't fail for a none existing volume")
	}

	oldLatest := "foo@bar-2010-10-21-14-01-36"
	driver.snapshots = []string{
		"foo@bar-2010-10-21-14-01-32",
		"foo@bar-2010-10-21-14-01-33",
		"foo@bar-2010-10-21-14-01-34",
		"foo@bar-2010-10-21-14-01-35",
		oldLatest,
	}
	err = TakeSnapshot("foo", "bar", 2, false, inboxPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(driver.snapshots) != 2 {
		t.Errorf("Keep argument of 2 didn't cleanup snapshots. %d were remaining", len(driver.snapshots))
	}
	if driver.snapshots[0] != oldLatest {
		t.Errorf("Didn't keep the right snapshot when cleaning up (kept %s, should have kept %s)", driver.snapshots[0], oldLatest)
	}

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
			b.snapshots = append(b.snapshots[:i], b.snapshots[i+1:]...)
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

	fmt.Fprint(output, "testsnapshot")
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