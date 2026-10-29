package file_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nostosx/servex-contrib/config/viper/file"
)

func TestViperFileFormats(t *testing.T) {
	for _, tt := range []struct{ name, content string }{
		{"config.json", "{\"http\":{\"port\":8080}}"},
		{"config.yaml", "http:\n  port: 8080\n"},
		{"config.toml", "[http]\nport = 8080\n"},
		{"config.env", "HTTP_PORT=8080\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), tt.name)
			if err := os.WriteFile(path, []byte(tt.content), 0600); err != nil {
				t.Fatal(err)
			}
			values, err := file.New(path).Load(t.Context())
			if err != nil || len(values) == 0 {
				t.Fatalf("Load=%v,%v", values, err)
			}
		})
	}
}
