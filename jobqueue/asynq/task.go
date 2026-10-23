package asynq

import (
	"context"
	"encoding/json/v2"
	"slices"

	"github.com/hibiken/asynq"

	"github.com/nostosx/servex/messaging/jobqueue"
	"github.com/nostosx/servex/metadata"
)

// Task headers used by the Queue. Their capital letters keep them apart from
// metadata keys, which are always lower case; they are not exposed in
// Job.Header.
const (
	// HeaderMetadata holds the JSON encoded Job.Header when one of its keys
	// has several values, which plain asynq headers cannot express.
	HeaderMetadata = "Servex-Metadata"
	// HeaderUnique holds the Redis key of a job-lifetime unique reservation.
	// Enqueue reclaims it after observing the task's terminal state.
	HeaderUnique = "Servex-Unique"
)

// encodeHeaders converts md to asynq task headers. Keys with a single value
// map to plain headers; if any key has several values the whole md is stored
// in HeaderMetadata instead. reservationKey is stored in HeaderUnique if not empty.
func encodeHeaders(md metadata.MD, reservationKey string) (map[string]string, error) {
	if len(md) == 0 && reservationKey == "" {
		return nil, nil
	}
	h := make(map[string]string, len(md)+1)
	multi := false
	for k, vs := range md.All() {
		switch len(vs) {
		case 0:
		case 1:
			h[k] = vs[0]
		default:
			multi = true
		}
	}
	if multi {
		data, err := json.Marshal(map[string][]string(md))
		if err != nil {
			return nil, err
		}
		h = map[string]string{HeaderMetadata: string(data)}
	}
	if reservationKey != "" {
		h[HeaderUnique] = reservationKey
	}
	return h, nil
}

// decodeHeaders is the inverse of encodeHeaders. It returns the job header
// and the Redis key of the job-lifetime unique reservation.
func decodeHeaders(h map[string]string) (metadata.MD, string) {
	reservationKey := h[HeaderUnique]
	if data, ok := h[HeaderMetadata]; ok {
		var m map[string][]string
		if err := json.Unmarshal([]byte(data), &m); err == nil {
			md := make(metadata.MD, len(m))
			for k, vs := range m {
				for _, v := range vs {
					md.Add(k, v)
				}
			}
			return md, reservationKey
		}
	}
	var md metadata.MD
	for k, v := range h {
		if k == HeaderUnique || k == HeaderMetadata {
			continue
		}
		if md == nil {
			md = make(metadata.MD, len(h))
		}
		md.Add(k, v)
	}
	return md, reservationKey
}

// toJob converts the task handled with ctx to a job and returns it together
// with the Redis key of its job-lifetime unique reservation.
func toJob(ctx context.Context, t *asynq.Task) (*jobqueue.Job, string) {
	md, reservationKey := decodeHeaders(t.Headers())
	job := &jobqueue.Job{
		Type:    t.Type(),
		Payload: slices.Clone(t.Payload()),
		Header:  md,
		Attempt: 1,
	}
	if id, ok := asynq.GetTaskID(ctx); ok {
		job.ID = id
	}
	if n, ok := asynq.GetRetryCount(ctx); ok {
		job.Attempt = n + 1
	}
	return job, reservationKey
}
