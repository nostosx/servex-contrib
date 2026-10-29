package client_test

import (
	"strings"
	"testing"

	sxclient "github.com/nostosx/servex-contrib/transport/client"
)

func TestClientRequiresExplicitSelector(t *testing.T) {
	client, err := sxclient.New(t.Context(), "http://127.0.0.1:8080")
	if client != nil || err == nil || !strings.Contains(err.Error(), "WithSelector") {
		t.Fatalf("NewClient=%v,%v", client, err)
	}
}
