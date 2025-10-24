package grpcurl

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// RequestParser processes input into messages.
type RequestParser interface {
	// Next parses input data into the given request message. If called after
	// input is exhausted, it returns io.EOF. If the caller re-uses the same
	// instance in multiple calls to Next, it should call msg.Reset() in between
	// each call.
	Next(msg proto.Message) error
	// NumRequests returns the number of messages that have been parsed and
	// returned by a call to Next.
	NumRequests() int
}

type jsonRequestParser struct {
	dec           *json.Decoder
	requestCount  int
	unmarshalOpts protojson.UnmarshalOptions
}

// NewJSONRequestParser returns a RequestParser that reads data in JSON format
// from the given reader. The given resolver is used to assist with decoding of
// google.protobuf.Any messages.
//
// Input data that contains more than one message should just include all
// messages concatenated (though whitespace is necessary to separate some kinds
// of values in JSON).
//
// If the given reader has no data, the returned parser will return io.EOF on
// the very first call.
// NewJSONRequestParser returns a RequestParser that parses JSON data from the given reader.
func NewJSONRequestParser(in io.Reader, resolver interface{}) RequestParser {
	return &jsonRequestParser{
		dec: json.NewDecoder(in),
	}
}

// NewJSONRequestParserWithUnmarshaler is like NewJSONRequestParser but
// accepts a protobuf unmarshaler instead of AnyResolver.
func NewJSONRequestParserWithUnmarshaler(in io.Reader, unmarshaler interface{}) RequestParser {
	return &jsonRequestParser{
		dec: json.NewDecoder(in),
	}
}

// newJSONRequestParserWithOptions creates a JSON request parser with custom unmarshal options.
func newJSONRequestParserWithOptions(in io.Reader, opts protojson.UnmarshalOptions) RequestParser {
	return &jsonRequestParser{
		dec:           json.NewDecoder(in),
		unmarshalOpts: opts,
	}
}

func (f *jsonRequestParser) Next(m proto.Message) error {
	var msg json.RawMessage
	if err := f.dec.Decode(&msg); err != nil {
		return err
	}
	f.requestCount++

	// Convert old-style FieldMask format to new canonical format
	msg = convertFieldMaskFormat(msg)

	// Use protojson for v2 JSON unmarshaling
	return f.unmarshalOpts.Unmarshal(msg, m)
}

func (f *jsonRequestParser) NumRequests() int {
	return f.requestCount
}

// convertFieldMaskFormat converts old-style FieldMask JSON format to the new canonical format.
// Old format: {"paths": ["field1", "field2"]} -> New format: "field1,field2"
// This maintains backward compatibility with grpcurl behavior prior to protoreflect v2 migration.
func convertFieldMaskFormat(data []byte) []byte {
	// Parse the JSON to find and convert FieldMask fields
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		// If it's not a JSON object, return as-is
		return data
	}

	converted := convertFieldMaskInMap(raw)
	if !converted {
		// No FieldMask found, return original
		return data
	}

	// Re-marshal with the converted FieldMask
	result, err := json.Marshal(raw)
	if err != nil {
		// If re-marshaling fails, return original
		return data
	}
	return result
}

// convertFieldMaskInMap recursively converts FieldMask objects to string format.
// Returns true if any conversion was made.
func convertFieldMaskInMap(obj map[string]interface{}) bool {
	converted := false
	for key, value := range obj {
		switch v := value.(type) {
		case map[string]interface{}:
			// Check if this looks like a FieldMask: has a "paths" field with an array
			if paths, ok := v["paths"].([]interface{}); ok && len(v) == 1 {
				// Convert paths array to comma-separated string
				pathStrings := make([]string, 0, len(paths))
				allStrings := true
				for _, p := range paths {
					if str, ok := p.(string); ok {
						pathStrings = append(pathStrings, str)
					} else {
						allStrings = false
						break
					}
				}
				if allStrings {
					// This is a FieldMask, convert it
					obj[key] = strings.Join(pathStrings, ",")
					converted = true
				} else {
					// Recurse into nested object
					if convertFieldMaskInMap(v) {
						converted = true
					}
				}
			} else {
				// Recurse into nested object
				if convertFieldMaskInMap(v) {
					converted = true
				}
			}
		case []interface{}:
			// Recurse into array elements that are objects
			for _, item := range v {
				if itemMap, ok := item.(map[string]interface{}); ok {
					if convertFieldMaskInMap(itemMap) {
						converted = true
					}
				}
			}
		}
	}
	return converted
}

const (
	textSeparatorChar = '\x1e'
)

type textRequestParser struct {
	r            *bufio.Reader
	err          error
	requestCount int
}

// NewTextRequestParser returns a RequestParser that reads data in the protobuf
// text format from the given reader.
//
// Input data that contains more than one message should include an ASCII
// 'Record Separator' character (0x1E) between each message.
//
// Empty text is a valid text format and represents an empty message. So if the
// given reader has no data, the returned parser will yield an empty message
// for the first call to Next and then return io.EOF thereafter. This also means
// that if the input data ends with a record separator, then a final empty
// message will be parsed *after* the separator.
func NewTextRequestParser(in io.Reader) RequestParser {
	return &textRequestParser{r: bufio.NewReader(in)}
}

func (f *textRequestParser) Next(m proto.Message) error {
	if f.err != nil {
		return f.err
	}

	var b []byte
	b, f.err = f.r.ReadBytes(textSeparatorChar)
	if f.err != nil && f.err != io.EOF {
		return f.err
	}
	// remove delimiter
	if len(b) > 0 && b[len(b)-1] == textSeparatorChar {
		b = b[:len(b)-1]
	}

	f.requestCount++

	return prototext.Unmarshal([]byte(string(b)), m)
}

func (f *textRequestParser) NumRequests() int {
	return f.requestCount
}

// Formatter translates messages into string representations.
type Formatter func(proto.Message) (string, error)

// NewJSONFormatter returns a formatter that returns JSON strings. The JSON will
// include empty/default values (instead of just omitted them) if emitDefaults
// is true. The given resolver is used to assist with encoding of
// google.protobuf.Any messages.
func NewJSONFormatter(emitDefaults bool, resolver interface{}) Formatter {
	// Use protojson for v2 JSON marshaling
	formatter := func(message proto.Message) (string, error) {
		opts := protojson.MarshalOptions{
			EmitUnpopulated: emitDefaults,
		}
		output, err := opts.Marshal(message)
		if err != nil {
			return "", err
		}
		var buf bytes.Buffer
		if err := json.Indent(&buf, output, "", "  "); err != nil {
			return "", err
		}
		return buf.String(), nil
	}
	return formatter
}

// NewTextFormatter returns a formatter that returns strings in the protobuf
// text format. If includeSeparator is true then, when invoked to format
// multiple messages, all messages after the first one will be prefixed with the
// ASCII 'Record Separator' character (0x1E).
func NewTextFormatter(includeSeparator bool) Formatter {
	tf := textFormatter{useSeparator: includeSeparator}
	return tf.format
}

type textFormatter struct {
	useSeparator bool
	numFormatted int
}

var protoTextMarshaler = prototext.MarshalOptions{}

func (tf *textFormatter) format(m proto.Message) (string, error) {
	var buf bytes.Buffer
	if tf.useSeparator && tf.numFormatted > 0 {
		if err := buf.WriteByte(textSeparatorChar); err != nil {
			return "", err
		}
	}

	// Use prototext for v2 text marshaling
	output, err := protoTextMarshaler.Marshal(m)
	if err != nil {
		return "", err
	}
	if _, err := buf.Write(output); err != nil {
		return "", err
	}

	// no trailing newline needed
	str := buf.String()
	if len(str) > 0 && str[len(str)-1] == '\n' {
		str = str[:len(str)-1]
	}

	tf.numFormatted++

	return str, nil
}

// Format of request data. The allowed values are 'json' or 'text'.
type Format string

const (
	// FormatJSON specifies input data in JSON format. Multiple request values
	// may be concatenated (messages with a JSON representation other than
	// object must be separated by whitespace, such as a newline)
	FormatJSON = Format("json")

	// FormatText specifies input data must be in the protobuf text format.
	// Multiple request values must be separated by the "record separator"
	// ASCII character: 0x1E. The stream should not end in a record separator.
	// If it does, it will be interpreted as a final, blank message after the
	// separator.
	FormatText = Format("text")
)

// AnyResolverFromDescriptorSource returns an AnyResolver that will search for
// types using the given descriptor source.
func AnyResolverFromDescriptorSource(source DescriptorSource) interface{} {
	return &anyResolver{source: source}
}

// AnyResolverFromDescriptorSourceWithFallback returns an AnyResolver that will
// search for types using the given descriptor source and then fallback to a
// special message if the type is not found. The fallback type will render to
// JSON with a "@type" property, just like an Any message, but also with a
// custom "@value" property that includes the binary encoded payload.
func AnyResolverFromDescriptorSourceWithFallback(source DescriptorSource) interface{} {
	res := anyResolver{source: source}
	return &anyResolverWithFallback{AnyResolver: &res}
}

type anyResolver struct {
	source DescriptorSource

	mu       sync.RWMutex
	resolved map[string]func() proto.Message
}

func (r *anyResolver) Resolve(typeUrl string) (proto.Message, error) {
	mname := typeUrl
	if slash := strings.LastIndex(mname, "/"); slash >= 0 {
		mname = mname[slash+1:]
	}

	r.mu.RLock()
	factory := r.resolved[mname]
	r.mu.RUnlock()

	// already resolved?
	if factory != nil {
		return factory(), nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// double-check, in case we were racing with another goroutine
	// that resolved this one
	factory = r.resolved[mname]
	if factory != nil {
		return factory(), nil
	}

	// use descriptor source to resolve message type
	d, err := r.source.FindSymbol(mname)
	if err != nil {
		return nil, err
	}
	md, ok := d.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("unknown message: %s", typeUrl)
	}

	factory = func() proto.Message {
		return dynamicpb.NewMessage(md)
	}
	if r.resolved == nil {
		r.resolved = map[string]func() proto.Message{}
	}
	r.resolved[mname] = factory
	return factory(), nil
}

// anyResolverWithFallback can provide a fallback value for unknown
// messages that will format itself to JSON using an "@value" field
// that has the base64-encoded data for the unknown message value.
type anyResolverWithFallback struct {
	AnyResolver interface{}
}

func (r anyResolverWithFallback) Resolve(typeUrl string) (proto.Message, error) {
	// For now, just return the fallback since we don't have a proper resolver
	return &unknownAny{TypeUrl: typeUrl, Error: fmt.Sprintf("%s is not recognized; see @value for raw binary message data", typeUrl)}, nil
}

type unknownAny struct {
	TypeUrl string `json:"@type"`
	Error   string `json:"@error"`
	Value   string `json:"@value"`
}

func (a *unknownAny) MarshalJSONPB(jsm interface{}) ([]byte, error) {
	return json.Marshal(a)
}

func (a *unknownAny) Unmarshal(b []byte) error {
	a.Value = base64.StdEncoding.EncodeToString(b)
	return nil
}

func (a *unknownAny) Reset() {
	a.Value = ""
}

func (a *unknownAny) ProtoReflect() protoreflect.Message {
	// Return a minimal implementation for unknownAny
	return dynamicpb.NewMessage(nil)
}

func (a *unknownAny) String() string {
	b, err := json.Marshal(a)
	if err != nil {
		return fmt.Sprintf("ERROR: %v", err.Error())
	}
	return string(b)
}

func (a *unknownAny) ProtoMessage() {
}

var _ proto.Message = (*unknownAny)(nil)

// FormatOptions is a set of flags that are passed to a JSON or text formatter.
type FormatOptions struct {
	// EmitJSONDefaultFields flag, when true, includes empty/default values in the output.
	// FormatJSON only flag.
	EmitJSONDefaultFields bool

	// AllowUnknownFields is an option for the parser. When true,
	// it accepts input which includes unknown fields. These unknown fields
	// are skipped instead of returning an error.
	// FormatJSON only flag.
	AllowUnknownFields bool

	// IncludeTextSeparator is true then, when invoked to format multiple messages,
	// all messages after the first one will be prefixed with the
	// ASCII 'Record Separator' character (0x1E).
	// It might be useful when the output is piped to another grpcurl process.
	// FormatText only flag.
	IncludeTextSeparator bool
}

// RequestParserAndFormatter returns a request parser and formatter for the
// given format. The given descriptor source may be used for parsing message
// data (if needed by the format).
// It accepts a set of options. The field EmitJSONDefaultFields and IncludeTextSeparator
// are options for JSON and protobuf text formats, respectively. The AllowUnknownFields field
// is a JSON-only format flag.
// Requests will be parsed from the given in.
func RequestParserAndFormatter(format Format, descSource DescriptorSource, in io.Reader, opts FormatOptions) (RequestParser, Formatter, error) {
	switch format {
	case FormatJSON:
		resolver := AnyResolverFromDescriptorSource(descSource)
		unmarshalOpts := protojson.UnmarshalOptions{
			DiscardUnknown: opts.AllowUnknownFields,
		}
		return newJSONRequestParserWithOptions(in, unmarshalOpts), NewJSONFormatter(opts.EmitJSONDefaultFields, anyResolverWithFallback{AnyResolver: resolver}), nil
	case FormatText:
		return NewTextRequestParser(in), NewTextFormatter(opts.IncludeTextSeparator), nil
	default:
		return nil, nil, fmt.Errorf("unknown format: %s", format)
	}
}

// RequestParserAndFormatterFor returns a request parser and formatter for the
// given format. The given descriptor source may be used for parsing message
// data (if needed by the format). The flags emitJSONDefaultFields and
// includeTextSeparator are options for JSON and protobuf text formats,
// respectively. Requests will be parsed from the given in.
// This function is deprecated. Please use RequestParserAndFormatter instead.
// DEPRECATED
func RequestParserAndFormatterFor(format Format, descSource DescriptorSource, emitJSONDefaultFields, includeTextSeparator bool, in io.Reader) (RequestParser, Formatter, error) {
	return RequestParserAndFormatter(format, descSource, in, FormatOptions{
		EmitJSONDefaultFields: emitJSONDefaultFields,
		IncludeTextSeparator:  includeTextSeparator,
	})
}

// DefaultEventHandler logs events to a writer. This is not thread-safe, but is
// safe for use with InvokeRPC as long as NumResponses and Status are not read
// until the call to InvokeRPC completes.
type DefaultEventHandler struct {
	Out       io.Writer
	Formatter Formatter
	// 0 = default
	// 1 = verbose
	// 2 = very verbose
	VerbosityLevel int

	// NumResponses is the number of responses that have been received.
	NumResponses int
	// Status is the status that was received at the end of an RPC. It is
	// nil if the RPC is still in progress.
	Status *status.Status
}

// NewDefaultEventHandler returns an InvocationEventHandler that logs events to
// the given output. If verbose is true, all events are logged. Otherwise, only
// response messages are logged.
//
// Deprecated: NewDefaultEventHandler exists for compatibility.
// It doesn't allow fine control over the `VerbosityLevel`
// and provides only 0 and 1 options (which corresponds to the `verbose` argument).
// Use DefaultEventHandler{} initializer directly.
func NewDefaultEventHandler(out io.Writer, descSource DescriptorSource, formatter Formatter, verbose bool) *DefaultEventHandler {
	verbosityLevel := 0
	if verbose {
		verbosityLevel = 1
	}
	return &DefaultEventHandler{
		Out:            out,
		Formatter:      formatter,
		VerbosityLevel: verbosityLevel,
	}
}

var _ InvocationEventHandler = (*DefaultEventHandler)(nil)

func (h *DefaultEventHandler) OnResolveMethod(md protoreflect.MethodDescriptor) {
	if h.VerbosityLevel > 0 {
		txt, err := GetDescriptorText(md, nil)
		if err == nil {
			fmt.Fprintf(h.Out, "\nResolved method descriptor:\n%s\n", txt)
		}
	}
}

func (h *DefaultEventHandler) OnSendHeaders(md metadata.MD) {
	if h.VerbosityLevel > 0 {
		fmt.Fprintf(h.Out, "\nRequest metadata to send:\n%s\n", MetadataToString(md))
	}
}

func (h *DefaultEventHandler) OnReceiveHeaders(md metadata.MD) {
	if h.VerbosityLevel > 0 {
		fmt.Fprintf(h.Out, "\nResponse headers received:\n%s\n", MetadataToString(md))
	}
}

func (h *DefaultEventHandler) OnReceiveResponse(resp proto.Message) {
	h.NumResponses++
	if h.VerbosityLevel > 1 {
		fmt.Fprintf(h.Out, "\nEstimated response size: %d bytes\n", proto.Size(resp))
	}
	if h.VerbosityLevel > 0 {
		fmt.Fprint(h.Out, "\nResponse contents:\n")
	}
	if respStr, err := h.Formatter(resp); err != nil {
		fmt.Fprintf(h.Out, "Failed to format response message %d: %v\n", h.NumResponses, err)
	} else {
		fmt.Fprintln(h.Out, respStr)
	}
}

func (h *DefaultEventHandler) OnReceiveTrailers(stat *status.Status, md metadata.MD) {
	h.Status = stat
	if h.VerbosityLevel > 0 {
		fmt.Fprintf(h.Out, "\nResponse trailers received:\n%s\n", MetadataToString(md))
	}
}

// PrintStatus prints details about the given status to the given writer. The given
// formatter is used to print any detail messages that may be included in the status.
// If the given status has a code of OK, "OK" is printed and that is all. Otherwise,
// "ERROR:" is printed along with a line showing the code, one showing the message
// string, and each detail message if any are present. The detail messages will be
// printed as proto text format or JSON, depending on the given formatter.
func PrintStatus(w io.Writer, stat *status.Status, formatter Formatter) {
	if stat.Code() == codes.OK {
		fmt.Fprintln(w, "OK")
		return
	}
	fmt.Fprintf(w, "ERROR:\n  Code: %s\n  Message: %s\n", stat.Code().String(), stat.Message())

	statpb := stat.Proto()
	if len(statpb.Details) > 0 {
		fmt.Fprintf(w, "  Details:\n")
		for i, det := range statpb.Details {
			prefix := fmt.Sprintf("  %d)", i+1)
			fmt.Fprintf(w, "%s\t", prefix)
			prefix = strings.Repeat(" ", len(prefix)) + "\t"

			output, err := formatter(det)
			if err != nil {
				fmt.Fprintf(w, "Error parsing detail message: %v\n", err)
			} else {
				lines := strings.Split(output, "\n")
				for i, line := range lines {
					if i == 0 {
						// first line is already indented
						fmt.Fprintf(w, "%s\n", line)
					} else {
						fmt.Fprintf(w, "%s%s\n", prefix, line)
					}
				}
			}
		}
	}
}
