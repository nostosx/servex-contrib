//go:build integration

package oauth2server

import (
	"crypto/rand"
	"crypto/rsa"
	"database/sql"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ory/fosite"
	"github.com/ory/fosite/handler/openid"
	"github.com/ory/fosite/storage"
)

func TestIntegrationPostgresDurableCredentialsAndAtomicRotation(t *testing.T) {
	dsn := os.Getenv("SERVEX_IT_POSTGRES_DSN")
	if dsn == "" {
		t.Fatal("SERVEX_IT_POSTGRES_DSN must refer to an owned disposable database")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := ApplyPostgresSchema(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	clients := storage.NewMemoryStore()
	clients.Clients["public"] = &fosite.DefaultClient{ID: "public", Public: true, RedirectURIs: []string{"https://client.example/callback"}, GrantTypes: []string{"authorization_code", "refresh_token"}, ResponseTypes: []string{"code"}, Scopes: []string{"openid", "profile", "offline_access"}}
	config := Config{Issuer: "https://issuer.example", Key: key, KeyID: "integration-key", OAuth: fosite.Config{GlobalSecret: []byte(strings.Repeat("s", 32)), AccessTokenLifespan: time.Minute, RefreshTokenLifespan: time.Hour, AuthorizeCodeLifespan: time.Minute}}
	first, err := New(config, &PostgresStore{DB: db, Clients: clients})
	if err != nil {
		t.Fatal(err)
	}
	verifier := strings.Repeat("v", 64)
	code := authorize(t, first, verifier)
	second, err := New(config, &PostgresStore{DB: db, Clients: clients})
	if err != nil {
		t.Fatal(err)
	}
	status, tokens := tokenRequest(t, second, url.Values{"grant_type": {"authorization_code"}, "client_id": {"public"}, "code": {code}, "code_verifier": {verifier}, "redirect_uri": {"https://client.example/callback"}})
	if status != 200 {
		t.Fatalf("durable authorization exchange returned %d", status)
	}
	refresh := tokens["refresh_token"].(string)
	var wait sync.WaitGroup
	wait.Add(2)
	statuses := make(chan int, 2)
	for range 2 {
		go func() {
			defer wait.Done()
			status, _ := tokenRequest(t, second, url.Values{"grant_type": {"refresh_token"}, "client_id": {"public"}, "refresh_token": {refresh}})
			statuses <- status
		}()
	}
	wait.Wait()
	close(statuses)
	success := 0
	for status := range statuses {
		if status == 200 {
			success++
		} else if status != 400 {
			t.Errorf("unexpected concurrent refresh status %d", status)
		}
	}
	if success != 1 {
		t.Fatalf("concurrent refresh produced %d successes", success)
	}
	status, result := tokenRequest(t, second, url.Values{"grant_type": {"refresh_token"}, "client_id": {"public"}, "refresh_token": {refresh}})
	if status != 400 || result["error"] != "invalid_grant" {
		t.Fatal("refresh reuse was not rejected")
	}
	if _, _, err := first.Provider.IntrospectToken(t.Context(), tokens["access_token"].(string), fosite.AccessToken, openid.NewDefaultSession()); err == nil {
		t.Fatal("rotated grant retained original access token")
	}
	var exposed int
	if err := db.QueryRowContext(t.Context(), `SELECT count(*) FROM oauth2_sessions WHERE key=$1 OR key=$2`, code, refresh).Scan(&exposed); err != nil {
		t.Fatal(err)
	}
	if exposed != 0 {
		t.Fatal("raw credential persisted as lookup key")
	}
}
