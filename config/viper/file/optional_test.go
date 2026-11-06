package file_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/nostosx/servex-contrib/config/viper/file"
)

func TestOptionalMissingFileAndInvalidContents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	if _, err := file.New(path).Load(t.Context()); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	source := file.New(path, file.WithOptional())
	if values, err := source.Load(t.Context()); err != nil || len(values) != 0 {
		t.Fatal(values, err)
	}
	if err := os.WriteFile(path, []byte("[invalid"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Load(t.Context()); err == nil {
		t.Fatal("optional file ignored a decoding error")
	}
}
