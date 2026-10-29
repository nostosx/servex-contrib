package httpx

import (
	"mime"
	"net/http"
	"strconv"
	"strings"

	jsoncodec "github.com/nostosx/servex-contrib/encoding/json"

	"github.com/nostosx/servex/encoding"
)

// codecs is an immutable set of codecs keyed by name, with JSON always
// present.
type codecs map[string]encoding.Codec

// newCodecs builds a set from jsoncodec.Codec followed by extra; later codecs
// replace earlier ones with the same name.
func newCodecs(extra []encoding.Codec) codecs {
	set := codecs{jsoncodec.Codec.Name(): jsoncodec.Codec}
	for _, c := range extra {
		if c != nil {
			set[c.Name()] = c
		}
	}
	return set
}

// json returns the codec registered as "json".
func (s codecs) json() encoding.Codec { return s[jsoncodec.Codec.Name()] }

// lookup returns the codec for a media type such as "application/json",
// "application/problem+json" or "application/x-protobuf".
func (s codecs) lookup(mediaType string) (encoding.Codec, bool) {
	name, ok := codecName(mediaType)
	if !ok {
		return nil, false
	}
	c, ok := s[name]
	return c, ok
}

// forResponse negotiates the response codec: the best Accept entry with a
// known codec wins, then the request Content-Type, then JSON.
func (s codecs) forResponse(r *http.Request) encoding.Codec {
	var (
		best  encoding.Codec
		bestQ float64
	)
	for _, accept := range r.Header.Values("Accept") {
		for part := range strings.SplitSeq(accept, ",") {
			mt, params, err := mime.ParseMediaType(strings.TrimSpace(part))
			if err != nil {
				continue
			}
			q := 1.0
			if v, ok := params["q"]; ok {
				if q, err = strconv.ParseFloat(v, 64); err != nil {
					continue
				}
			}
			if q <= bestQ {
				continue
			}
			if c, ok := s.lookup(mt); ok {
				best, bestQ = c, q
			}
		}
	}
	if best != nil {
		return best
	}
	if mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err == nil {
		if c, ok := s.lookup(mt); ok {
			return c
		}
	}
	return s.json()
}

// codecName derives a codec name from a media type: the structured syntax
// suffix if any ("problem+json" -> "json"), without an "x-" prefix
// ("x-yaml" -> "yaml"), with "protobuf" shortened to "proto".
func codecName(mediaType string) (string, bool) {
	typ, sub, ok := strings.Cut(strings.ToLower(mediaType), "/")
	if !ok || typ == "*" || sub == "" || sub == "*" {
		return "", false
	}
	if _, suffix, ok := strings.CutLast(sub, "+"); ok {
		sub = suffix
	}
	sub = strings.TrimPrefix(sub, "x-")
	if sub == "protobuf" {
		sub = "proto"
	}
	return sub, sub != ""
}

// contentType returns the media type advertised for c.
func contentType(c encoding.Codec) string {
	return "application/" + c.Name()
}
