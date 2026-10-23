package etcd

import (
	"errors"
	"fmt"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/nostosx/servex/errorx"
)

// leaseCount returns the number of leases granted on the server that cli is
// connected to.
func leaseCount(t *testing.T, cli *clientv3.Client) int {
	t.Helper()
	resp, err := cli.Leases(t.Context())
	if err != nil {
		t.Fatalf("Leases() = %v", err)
	}
	return len(resp.Leases)
}

func TestRegisterFailureRevokesGrantedLease(t *testing.T) {
	tests := []struct {
		name   string
		inject func(f *faults)
	}{
		{"put fails", func(f *faults) { f.put.Store(true) }},
		{"keep-alive fails", func(f *faults) { f.keepAlive.Store(true) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, cli, f := faultyRegistry(t)
			tt.inject(f)
			err := r.Register(t.Context(), v1())
			if errorx.CodeOf(err) != errorx.Unavailable || !errorx.IsRetryable(err) || !errors.Is(err, errInjected) {
				t.Fatalf("Register() = %v, want retryable Unavailable caused by the injected failure", err)
			}
			// The lease granted for the failed registration is revoked at
			// once instead of expiring after its TTL, which also deletes a
			// key written with it.
			if n := leaseCount(t, cli); n != 0 {
				t.Errorf("leases after the failed Register = %d, want 0", n)
			}
			key := r.key("svc", "1")
			if resp, err := cli.Get(t.Context(), key); err != nil || len(resp.Kvs) != 0 {
				t.Errorf("Get(%s) = %v, %v, want no key", key, resp, err)
			}
			if _, ok := r.keepers[key]; ok {
				t.Error("a keeper is tracked for the failed registration")
			}
			// The registry recovers with the client.
			f.put.Store(false)
			f.keepAlive.Store(false)
			mustRegister(t, r, v1())
			if n := leaseCount(t, cli); n != 1 {
				t.Errorf("leases after Register = %d, want 1", n)
			}
		})
	}
}

func TestKeeperRetriesUntilWritesSucceed(t *testing.T) {
	r, cli, f := faultyRegistry(t)
	key := r.key("svc", "1")
	mustRegister(t, r, v1())
	old := r.keepers[key].leaseID()

	// The lease is lost while writes fail: the keeper retries with backoff,
	// and each failed attempt revokes the lease it granted. Without the
	// revocation the leases of the attempts, which outlive the retry
	// interval, would pile up.
	f.put.Store(true)
	if _, err := cli.Revoke(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	most := 0
	for deadline := time.Now().Add(5 * minRetry); time.Now().Before(deadline); {
		most = max(most, leaseCount(t, cli))
		time.Sleep(minRetry / 10)
	}
	if most > 1 {
		t.Fatalf("leases while writes fail = %d, want at most the one of the current attempt", most)
	}

	f.put.Store(false)
	eventually(t, func() (any, error) {
		resp, err := cli.Get(t.Context(), key)
		if err != nil {
			return nil, fmt.Errorf("Get(%s): %w", key, err)
		}
		if len(resp.Kvs) != 1 {
			return nil, fmt.Errorf("Get(%s) = %v, want the restored instance", key, resp)
		}
		if lease := clientv3.LeaseID(resp.Kvs[0].Lease); lease == old || lease != r.keepers[key].leaseID() {
			return nil, fmt.Errorf("stored lease %x, want a new tracked lease (old %x)", lease, old)
		}
		return nil, nil
	})
}
