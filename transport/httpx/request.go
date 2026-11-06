package httpx

import (
	"context"
	"errors"
	"net/http"

	"github.com/nostosx/servex/errorx"
)

func (rt *route) decodeRequest(ctx context.Context, r *http.Request, req any) error {
	if rt.decoder == nil {
		return rt.bind(r, req)
	}
	decoded := r.WithContext(ctx)
	defer func() {
		// WithContext copies parsing fields. Publish them for response handling
		// and transport-owned multipart cleanup, including on decoder failure.
		r.Form, r.PostForm, r.MultipartForm = decoded.Form, decoded.PostForm, decoded.MultipartForm
	}()
	if err := rt.decoder(decoded, req); err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			return bodyTooLarge(err)
		}
		if _, ok := errors.AsType[*errorx.Error](err); ok {
			return err
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return errorx.New(errorx.InvalidArgument, "DECODE_REQUEST", "failed to decode the request").WithCause(err)
	}
	return nil
}
