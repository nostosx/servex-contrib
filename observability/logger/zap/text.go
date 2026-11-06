package zap

import (
	"encoding/json"
	"maps"
	"path"
	"slices"
	"strings"
	"unicode"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var textBuffers = buffer.NewPool()

// textEncoder presents structured fields as bracketed details on one console line.
type textEncoder struct {
	zapcore.Encoder
	labels map[string]string
}

func newTextEncoder(cfg zapcore.EncoderConfig, labels map[string]string) zapcore.Encoder {
	return &textEncoder{Encoder: zapcore.NewJSONEncoder(cfg), labels: maps.Clone(labels)}
}

func (e *textEncoder) Clone() zapcore.Encoder {
	return &textEncoder{Encoder: e.Encoder.Clone(), labels: e.labels}
}

func (e *textEncoder) EncodeEntry(entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	encoded, err := e.Encoder.EncodeEntry(entry, fields)
	if err != nil {
		return nil, err
	}
	defer encoded.Free()
	var values map[string]json.RawMessage
	if err := json.Unmarshal(encoded.Bytes(), &values); err != nil {
		return nil, err
	}
	out := textBuffers.Get()
	out.AppendString(consoleValue(values["timestamp"]))
	out.AppendByte('\t')
	out.AppendString(consoleValue(values["level"]))
	out.AppendByte('\t')
	out.AppendString(consoleValue(values["caller"]))
	out.AppendByte('\t')
	component := consoleValue(values["component"])
	if component == "" && entry.Caller.Defined {
		component = path.Base(path.Dir(entry.Caller.File))
	}
	if component != "" {
		out.AppendString("[" + componentName(component) + "] ")
	}
	out.AppendString(consoleValue(values["msg"]))
	for _, key := range []string{"timestamp", "level", "caller", "component", "msg"} {
		delete(values, key)
	}
	keys := make([]string, 0, len(values))
	for _, key := range []string{"operation", "method", "duration", "status", "error", "error.message", "trace_id"} {
		if _, ok := values[key]; ok {
			keys = append(keys, key)
		}
	}
	remaining := make([]string, 0, len(values))
	for key := range values {
		if !slices.Contains(keys, key) {
			remaining = append(remaining, key)
		}
	}
	slices.Sort(remaining)
	keys = append(keys, remaining...)
	for _, key := range keys {
		label := key
		if configured, ok := e.labels[key]; ok {
			label = configured
		}
		out.AppendString(" [" + singleLine(label) + ":" + consoleValue(values[key]) + "]")
	}
	out.AppendByte('\n')
	return out, nil
}

func componentName(value string) string {
	switch strings.ToLower(value) {
	case "rpc", "http", "grpc", "sql", "sdk", "ai", "nats":
		return strings.ToUpper(value)
	default:
		runes := []rune(value)
		if len(runes) != 0 {
			runes[0] = unicode.ToUpper(runes[0])
		}
		return singleLine(string(runes))
	}
}

func consoleValue(value json.RawMessage) string {
	var text string
	if len(value) > 0 && value[0] == '"' {
		_ = json.Unmarshal(value, &text)
	} else {
		text = string(value)
	}
	return singleLine(text)
}

func singleLine(value string) string {
	return strings.NewReplacer("\r", "\\r", "\n", "\\n", "\t", "\\t").Replace(value)
}
