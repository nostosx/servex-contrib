package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/ory/fosite"
	"github.com/ory/fosite/handler/openid"
	"github.com/ory/fosite/storage"
)

// PostgresSchema is applied explicitly by the application's migration command.
// The schema stores only digests of code/token lookup keys and sanitized requests.
const PostgresSchema = `CREATE TABLE IF NOT EXISTS oauth2_sessions (
 kind text NOT NULL, key text NOT NULL, request_id text NOT NULL,
 client_id text NOT NULL, subject text NOT NULL, active boolean NOT NULL DEFAULT true,
 expires_at timestamptz NOT NULL, data jsonb NOT NULL, PRIMARY KEY(kind,key));
 CREATE INDEX IF NOT EXISTS oauth2_sessions_request ON oauth2_sessions(request_id);
 CREATE INDEX IF NOT EXISTS oauth2_sessions_subject ON oauth2_sessions(subject);
 CREATE INDEX IF NOT EXISTS oauth2_sessions_client ON oauth2_sessions(client_id);
 CREATE INDEX IF NOT EXISTS oauth2_sessions_expiry ON oauth2_sessions(expires_at);`

// ApplyPostgresSchema is an explicit migration operation. An advisory transaction
// lock serializes concurrent migration commands; constructors never call it.
func ApplyPostgresSchema(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('nostosx.oauth2server.schema',0))`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, PostgresSchema); err != nil {
		return err
	}
	return tx.Commit()
}

// PostgresStore persists native openid.DefaultSession requests across replicas.
// Clients and Check belong to the application; Check revalidates account/session
// revisions and current grants on every stored credential read.
type PostgresStore struct {
	DB      *sql.DB
	Clients fosite.ClientManager
	Check   func(context.Context, fosite.Requester) error
}

var _ Store = (*PostgresStore)(nil)
var _ storage.Transactional = (*PostgresStore)(nil)

func (s *PostgresStore) GetClient(ctx context.Context, id string) (fosite.Client, error) {
	return s.Clients.GetClient(ctx, id)
}
func (s *PostgresStore) ClientAssertionJWTValid(context.Context, string) error {
	return fosite.ErrRequestUnauthorized
}
func (s *PostgresStore) SetClientAssertionJWT(context.Context, string, time.Time) error {
	return fosite.ErrRequestUnauthorized
}

type transactionKey struct{ store *PostgresStore }
type sqlExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *PostgresStore) executor(ctx context.Context) sqlExecutor {
	if tx, ok := ctx.Value(transactionKey{s}).(*sql.Tx); ok {
		return tx
	}
	return s.DB
}
func (s *PostgresStore) BeginTX(ctx context.Context) (context.Context, error) {
	if ctx.Value(transactionKey{s}) != nil {
		return nil, errors.New("oauth2 server: nested transaction")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return context.WithValue(ctx, transactionKey{s}, tx), nil
}
func (s *PostgresStore) Commit(ctx context.Context) error {
	tx, ok := ctx.Value(transactionKey{s}).(*sql.Tx)
	if !ok {
		return errors.New("oauth2 server: no transaction")
	}
	return tx.Commit()
}
func (s *PostgresStore) Rollback(ctx context.Context) error {
	tx, ok := ctx.Value(transactionKey{s}).(*sql.Tx)
	if !ok {
		return errors.New("oauth2 server: no transaction")
	}
	err := tx.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}
	return err
}

func digest(key string) string { sum := sha256.Sum256([]byte(key)); return hex.EncodeToString(sum[:]) }

type storedRequest struct {
	ClientID string         `json:"client_id"`
	Request  fosite.Request `json:"request"`
}

func (s *PostgresStore) create(ctx context.Context, kind, key string, request fosite.Requester) error {
	if _, ok := request.GetSession().(*openid.DefaultSession); !ok {
		return errors.New("oauth2 server: openid.DefaultSession required")
	}
	copy := fosite.NewRequest()
	copy.Merge(request)
	clientID := request.GetClient().GetID()
	copy.Client = nil
	// Remove credentials even if a caller accidentally supplies an unsanitized request.
	copy.Form = request.Sanitize([]string{"redirect_uri", "code_challenge", "code_challenge_method", "nonce", "prompt", "max_age", "acr_values"}).GetRequestForm()
	data, err := json.Marshal(storedRequest{ClientID: clientID, Request: *copy})
	if err != nil {
		return err
	}
	tokenType := fosite.AuthorizeCode
	if kind == "access" {
		tokenType = fosite.AccessToken
	}
	if kind == "refresh" {
		tokenType = fosite.RefreshToken
	}
	expires := request.GetSession().GetExpiresAt(tokenType)
	if expires.IsZero() {
		return errors.New("oauth2 server: bounded credential expiry required")
	}
	_, err = s.executor(ctx).ExecContext(ctx, `INSERT INTO oauth2_sessions(kind,key,request_id,client_id,subject,expires_at,data) VALUES($1,$2,$3,$4,$5,$6,$7)`, kind, digest(key), request.GetID(), clientID, request.GetSession().GetSubject(), expires, data)
	return err
}

func (s *PostgresStore) get(ctx context.Context, kind, key string) (fosite.Requester, error) {
	var data []byte
	var active bool
	var expires time.Time
	err := s.executor(ctx).QueryRowContext(ctx, `SELECT data,active,expires_at FROM oauth2_sessions WHERE kind=$1 AND key=$2`, kind, digest(key)).Scan(&data, &active, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fosite.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if !time.Now().Before(expires) {
		return nil, fosite.ErrNotFound
	}
	var stored storedRequest
	stored.Request.Session = openid.NewDefaultSession()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&stored); err != nil {
		return nil, err
	}
	client, err := s.Clients.GetClient(ctx, stored.ClientID)
	if err != nil {
		return nil, err
	}
	stored.Request.Client = client
	request := &stored.Request
	if !active {
		if kind == "code" {
			return request, fosite.ErrInvalidatedAuthorizeCode
		}
		return request, fosite.ErrInactiveToken
	}
	if s.Check != nil {
		if err := s.Check(ctx, request); err != nil {
			return nil, err
		}
	}
	return request, nil
}

func (s *PostgresStore) remove(ctx context.Context, kind, key string) error {
	_, err := s.executor(ctx).ExecContext(ctx, `DELETE FROM oauth2_sessions WHERE kind=$1 AND key=$2`, kind, digest(key))
	return err
}
func (s *PostgresStore) invalidate(ctx context.Context, kind, key string) error {
	result, err := s.executor(ctx).ExecContext(ctx, `UPDATE oauth2_sessions SET active=false WHERE kind=$1 AND key=$2 AND active=true`, kind, digest(key))
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fosite.ErrInactiveToken
	}
	return nil
}

func (s *PostgresStore) CreateAuthorizeCodeSession(ctx context.Context, key string, r fosite.Requester) error {
	return s.create(ctx, "code", key, r)
}
func (s *PostgresStore) GetAuthorizeCodeSession(ctx context.Context, key string, _ fosite.Session) (fosite.Requester, error) {
	return s.get(ctx, "code", key)
}
func (s *PostgresStore) InvalidateAuthorizeCodeSession(ctx context.Context, key string) error {
	return s.invalidate(ctx, "code", key)
}
func (s *PostgresStore) CreateAccessTokenSession(ctx context.Context, key string, r fosite.Requester) error {
	return s.create(ctx, "access", key, r)
}
func (s *PostgresStore) GetAccessTokenSession(ctx context.Context, key string, _ fosite.Session) (fosite.Requester, error) {
	return s.get(ctx, "access", key)
}
func (s *PostgresStore) DeleteAccessTokenSession(ctx context.Context, key string) error {
	return s.remove(ctx, "access", key)
}
func (s *PostgresStore) CreateRefreshTokenSession(ctx context.Context, key, _ string, r fosite.Requester) error {
	return s.create(ctx, "refresh", key, r)
}
func (s *PostgresStore) GetRefreshTokenSession(ctx context.Context, key string, _ fosite.Session) (fosite.Requester, error) {
	return s.get(ctx, "refresh", key)
}
func (s *PostgresStore) DeleteRefreshTokenSession(ctx context.Context, key string) error {
	return s.remove(ctx, "refresh", key)
}
func (s *PostgresStore) CreateOpenIDConnectSession(ctx context.Context, key string, r fosite.Requester) error {
	return s.create(ctx, "openid", key, r)
}
func (s *PostgresStore) GetOpenIDConnectSession(ctx context.Context, key string, _ fosite.Requester) (fosite.Requester, error) {
	return s.get(ctx, "openid", key)
}
func (s *PostgresStore) DeleteOpenIDConnectSession(ctx context.Context, key string) error {
	return s.remove(ctx, "openid", key)
}
func (s *PostgresStore) CreatePKCERequestSession(ctx context.Context, key string, r fosite.Requester) error {
	return s.create(ctx, "pkce", key, r)
}
func (s *PostgresStore) GetPKCERequestSession(ctx context.Context, key string, _ fosite.Session) (fosite.Requester, error) {
	return s.get(ctx, "pkce", key)
}
func (s *PostgresStore) DeletePKCERequestSession(ctx context.Context, key string) error {
	return s.remove(ctx, "pkce", key)
}

func (s *PostgresStore) RevokeRefreshToken(ctx context.Context, requestID string) error {
	return s.revoke(ctx, "refresh", requestID)
}
func (s *PostgresStore) RevokeAccessToken(ctx context.Context, requestID string) error {
	return s.revoke(ctx, "access", requestID)
}
func (s *PostgresStore) revoke(ctx context.Context, kind, requestID string) error {
	_, err := s.executor(ctx).ExecContext(ctx, `UPDATE oauth2_sessions SET active=false WHERE kind=$1 AND request_id=$2`, kind, requestID)
	return err
}
func (s *PostgresStore) RotateRefreshToken(ctx context.Context, requestID, signature string) error {
	if err := s.invalidate(ctx, "refresh", signature); err != nil {
		return err
	}
	return s.RevokeAccessToken(ctx, requestID)
}
