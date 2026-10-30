// Package cors provides explicit browser-origin policy using rs/cors. Mount it
// outside HTTP routing so preflight requests are handled before authentication.
package cors

import (
	"errors"
	"net/http"
	"slices"
	"strings"

	sdk "github.com/rs/cors"
)

// New requires an explicit origin policy. SDK options and callbacks are
// preserved; callbacks must be safe for concurrent use. Credentials with a
// wildcard origin are rejected. CORS does not provide authentication or CSRF.
func New(options sdk.Options) (func(http.Handler) http.Handler, error) {
	hasCallback := options.AllowOriginFunc != nil || options.AllowOriginVaryRequestFunc != nil
	if len(options.AllowedOrigins) == 0 && !hasCallback {
		return nil, errors.New("cors: explicit allowed origins or callback required")
	}
	for _, origin := range options.AllowedOrigins {
		if strings.TrimSpace(origin) == "" {
			return nil, errors.New("cors: empty origin")
		}
		if origin == "*" && options.AllowCredentials {
			return nil, errors.New("cors: wildcard origin with credentials")
		}
	}
	options.AllowedOrigins = slices.Clone(options.AllowedOrigins)
	options.AllowedMethods = slices.Clone(options.AllowedMethods)
	options.AllowedHeaders = slices.Clone(options.AllowedHeaders)
	options.ExposedHeaders = slices.Clone(options.ExposedHeaders)
	return sdk.New(options).Handler, nil
}
