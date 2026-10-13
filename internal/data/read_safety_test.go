package data

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestOpen_ConcurrentSymlinkSwapCannotEscape(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "app-data")
	if err := os.Mkdir(dataDir, 0o750); err != nil {
		t.Fatal(err)
	}
	const public = "app-owned data"
	const secret = "synthetic host-only data"
	source := filepath.Join(dataDir, "source.txt")
	outside := filepath.Join(root, "host-only.txt")
	for name, content := range map[string]string{source: public, outside: secret} {
		if err := os.WriteFile(name, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	slot := filepath.Join(dataDir, "report.txt")
	replacement := filepath.Join(dataDir, "replacement")
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			// Atomic replacement makes reads race with either an app-owned file
			// or a symlink to the host-only file.
			if err := os.Link(source, replacement); err != nil {
				t.Error(err)
				return
			}
			if err := os.Rename(replacement, slot); err != nil {
				t.Error(err)
				return
			}
			if err := os.Symlink(outside, replacement); err != nil {
				t.Error(err)
				return
			}
			if err := os.Rename(replacement, slot); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	defer func() { close(stop); wg.Wait() }()
	reads := 0
	for i := 0; i < 10000; i++ {
		f, _, err := Open(dataDir, "report.txt")
		if err != nil {
			// Missing paths and rejected swaps are safe.
			continue
		}
		content, err := io.ReadAll(f)
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		if string(content) != public {
			t.Fatalf("download escaped its app-data directory: got %q", content)
		}
		reads++
	}
	if reads == 0 {
		t.Fatal("no legitimate reads succeeded during the swap test")
	}
}

func TestOpen_PreservesReadContract(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "app-data")
	if err := os.MkdirAll(filepath.Join(dataDir, "nested"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "nested", "report.txt"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("nested/report.txt", filepath.Join(dataDir, "link.txt")); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, path string
		want       error
	}{
		{"missing", "missing.txt", ErrFileNotFound},
		{"directory", "nested", ErrNotAFile},
		{"traversal", "../host-only.txt", ErrInvalidPath},
		{"reserved", ".shinyhub-upload-tmp/file", ErrInvalidPath},
		{"contained symlink", "link.txt", ErrInvalidPath},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _, err := Open(dataDir, tc.path)
			if f != nil {
				f.Close()
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("Open(%q) error = %v, want %v", tc.path, err, tc.want)
			}
		})
	}
	t.Run("missing root", func(t *testing.T) {
		_, _, err := Open(filepath.Join(root, "absent"), "report.txt")
		if !errors.Is(err, ErrFileNotFound) {
			t.Fatalf("error = %v, want ErrFileNotFound", err)
		}
	})
	t.Run("operator symlinked root", func(t *testing.T) {
		alias := filepath.Join(root, "volume-alias")
		if err := os.Symlink(dataDir, alias); err != nil {
			t.Fatal(err)
		}
		f, fi, err := Open(alias, "nested/report.txt")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		content, err := io.ReadAll(f)
		if err != nil || string(content) != "data" || fi.Size() != 4 {
			t.Fatalf("content=%q stat=%v error=%v", content, fi, err)
		}
	})
}
