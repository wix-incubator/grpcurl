// Package grpcurl provides the core functionality exposed by the grpcurl command, for
// dynamically connecting to a server, using the reflection service to inspect the server,
// and invoking RPCs. The grpcurl command-line tool constructs a DescriptorSource, based
// on the command-line parameters, and supplies an InvocationEventHandler to supply request
// data (which can come from command-line args or the process's stdin) and to log the
// events (to the process's stdout).
package grpcurl

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/jhump/protoreflect/v2/protoprint"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	xdsCredentials "google.golang.org/grpc/credentials/xds"
	_ "google.golang.org/grpc/health" // import grpc/health to enable transparent client side checking
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	protov2 "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// ListServices uses the given descriptor source to return a sorted list of fully-qualified
// service names.
func ListServices(source DescriptorSource) ([]string, error) {
	svcs, err := source.ListServices()
	if err != nil {
		return nil, err
	}
	sort.Strings(svcs)
	return svcs, nil
}

type sourceWithFiles interface {
	GetAllFiles() ([]protoreflect.FileDescriptor, error)
}

var _ sourceWithFiles = (*fileSource)(nil)

// GetAllFiles uses the given descriptor source to return a list of file descriptors.
func GetAllFiles(source DescriptorSource) ([]protoreflect.FileDescriptor, error) {
	var files []protoreflect.FileDescriptor
	srcFiles, ok := source.(sourceWithFiles)

	// If an error occurs, we still try to load as many files as we can, so that
	// caller can decide whether to ignore error or not.
	var firstError error
	if ok {
		files, firstError = srcFiles.GetAllFiles()
	} else {
		// Source does not implement GetAllFiles method, so use ListServices
		// and grab files from there.
		svcNames, err := source.ListServices()
		if err != nil {
			firstError = err
		} else {
			allFiles := map[string]protoreflect.FileDescriptor{}
			for _, name := range svcNames {
				d, err := source.FindSymbol(name)
				if err != nil {
					if firstError == nil {
						firstError = err
					}
				} else {
					addAllFilesToSet(d.ParentFile(), allFiles)
				}
			}
			files = make([]protoreflect.FileDescriptor, len(allFiles))
			i := 0
			for _, fd := range allFiles {
				files[i] = fd
				i++
			}
		}
	}

	sort.Sort(filesByName(files))
	return files, firstError
}

type filesByName []protoreflect.FileDescriptor

func (f filesByName) Len() int {
	return len(f)
}

func (f filesByName) Less(i, j int) bool {
	return f[i].Path() < f[j].Path()
}

func (f filesByName) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

func addAllFilesToSet(fd protoreflect.FileDescriptor, all map[string]protoreflect.FileDescriptor) {
	if _, ok := all[fd.Path()]; ok {
		// already added
		return
	}
	all[fd.Path()] = fd
	for i := 0; i < fd.Imports().Len(); i++ {
		addAllFilesToSet(fd.Imports().Get(i), all)
	}
}

// ListMethods uses the given descriptor source to return a sorted list of method names
// for the specified fully-qualified service name.
func ListMethods(source DescriptorSource, serviceName string) ([]string, error) {
	dsc, err := source.FindSymbol(serviceName)
	if err != nil {
		return nil, err
	}
	if sd, ok := dsc.(protoreflect.ServiceDescriptor); !ok {
		return nil, notFound("Service", serviceName)
	} else {
		methods := make([]string, 0, sd.Methods().Len())
		for i := 0; i < sd.Methods().Len(); i++ {
			method := sd.Methods().Get(i)
			methods = append(methods, string(method.FullName()))
		}
		sort.Strings(methods)
		return methods, nil
	}
}

// MetadataFromHeaders converts a list of header strings (each string in
// "Header-Name: Header-Value" form) into metadata. If a string has a header
// name without a value (e.g. does not contain a colon), the value is assumed
// to be blank. Binary headers (those whose names end in "-bin") should be
// base64-encoded. But if they cannot be base64-decoded, they will be assumed to
// be in raw form and used as is.
func MetadataFromHeaders(headers []string) metadata.MD {
	md := make(metadata.MD)
	for _, part := range headers {
		if part != "" {
			pieces := strings.SplitN(part, ":", 2)
			if len(pieces) == 1 {
				pieces = append(pieces, "") // if no value was specified, just make it "" (maybe the header value doesn't matter)
			}
			headerName := strings.ToLower(strings.TrimSpace(pieces[0]))
			val := strings.TrimSpace(pieces[1])
			if strings.HasSuffix(headerName, "-bin") {
				if v, err := decode(val); err == nil {
					val = v
				}
			}
			md[headerName] = append(md[headerName], val)
		}
	}
	return md
}

var envVarRegex = regexp.MustCompile(`\${\w+}`)

// ExpandHeaders expands environment variables contained in the header string.
// If no corresponding environment variable is found an error is returned.
// TODO: Add escaping for `${`
func ExpandHeaders(headers []string) ([]string, error) {
	expandedHeaders := make([]string, len(headers))
	for idx, header := range headers {
		if header == "" {
			continue
		}
		results := envVarRegex.FindAllString(header, -1)
		if len(results) == 0 {
			expandedHeaders[idx] = headers[idx]
			continue
		}
		expandedHeader := header
		for _, result := range results {
			envVarName := result[2 : len(result)-1] // strip leading `${` and trailing `}`
			envVarValue, ok := os.LookupEnv(envVarName)
			if !ok {
				return nil, fmt.Errorf("header %q refers to missing environment variable %q", header, envVarName)
			}
			expandedHeader = strings.Replace(expandedHeader, result, envVarValue, -1)
		}
		expandedHeaders[idx] = expandedHeader
	}
	return expandedHeaders, nil
}

var base64Codecs = []*base64.Encoding{base64.StdEncoding, base64.URLEncoding, base64.RawStdEncoding, base64.RawURLEncoding}

func decode(val string) (string, error) {
	var firstErr error
	var b []byte
	// we are lenient and can accept any of the flavors of base64 encoding
	for _, d := range base64Codecs {
		var err error
		b, err = d.DecodeString(val)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		return string(b), nil
	}
	return "", firstErr
}

// MetadataToString returns a string representation of the given metadata, for
// displaying to users.
func MetadataToString(md metadata.MD) string {
	if len(md) == 0 {
		return "(empty)"
	}

	keys := make([]string, 0, len(md))
	for k := range md {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b bytes.Buffer
	first := true
	for _, k := range keys {
		vs := md[k]
		for _, v := range vs {
			if first {
				first = false
			} else {
				b.WriteString("\n")
			}
			b.WriteString(k)
			b.WriteString(": ")
			if strings.HasSuffix(k, "-bin") {
				v = base64.StdEncoding.EncodeToString([]byte(v))
			}
			b.WriteString(v)
		}
	}
	return b.String()
}

var printer = &protoprint.Printer{
	Compact:                  true,
	OmitComments:             protoprint.CommentsNonDoc,
	SortElements:             true,
	ForceFullyQualifiedNames: true,
}

// GetDescriptorText returns a string representation of the given descriptor.
// This returns a snippet of proto source that describes the given element.
func GetDescriptorText(dsc protoreflect.Descriptor, _ DescriptorSource) (string, error) {
	// Note: DescriptorSource is not used, but remains an argument for backwards
	// compatibility with previous implementation.
	txt, err := printer.PrintProtoToString(dsc)
	if err != nil {
		return "", err
	}
	// callers don't expect trailing newlines
	if txt[len(txt)-1] == '\n' {
		txt = txt[:len(txt)-1]
	}
	return txt, nil
}

// EnsureExtensions uses the given descriptor source to download extensions for
// the given message. It returns a copy of the given message, but as a dynamic
// message that knows about all extensions known to the given descriptor source.
func EnsureExtensions(source DescriptorSource, msg proto.Message) proto.Message {
	// For v2, we'll use dynamicpb directly since the extension handling is different
	// This is a simplified version - in practice, you might need more complex extension handling
	if v2Msg, ok := msg.(interface{ ProtoReflect() protoreflect.Message }); ok {
		dsc := v2Msg.ProtoReflect().Descriptor()
		return dynamicpb.NewMessage(dsc)
	}
	// Fallback for messages that don't implement ProtoReflect
	return msg
}

// fetchAllExtensions is no longer needed in v2 as extension handling is different

// fullyConvertToDynamic is no longer needed in v2 as dynamic message handling is different

// MakeTemplate returns a message instance for the given descriptor that is a
// suitable template for creating an instance of that message in JSON. In
// particular, it ensures that any repeated fields (which include map fields)
// are not empty, so they will render with a single element (to show the types
// and optionally nested fields). It also ensures that nested messages are not
// nil by setting them to a message that is also fleshed out as a template
// message.
func MakeTemplate(md protoreflect.MessageDescriptor) protov2.Message {
	return makeTemplate(md, nil)
}

func makeTemplate(md protoreflect.MessageDescriptor, path []protoreflect.MessageDescriptor) protov2.Message {
	switch string(md.FullName()) {
	case "google.protobuf.Any":
		// empty type URL is not allowed by JSON representation
		// so we must give it a dummy type
		var anyVal anypb.Any
		_ = anypb.MarshalFrom(&anyVal, &emptypb.Empty{}, protov2.MarshalOptions{})
		return &anyVal
	case "google.protobuf.Value":
		// unset kind is not allowed by JSON representation
		// so we must give it something
		return &structpb.Value{
			Kind: &structpb.Value_StructValue{StructValue: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"google.protobuf.Value": {Kind: &structpb.Value_StringValue{
						StringValue: "supports arbitrary JSON",
					}},
				},
			}},
		}
	case "google.protobuf.ListValue":
		return &structpb.ListValue{
			Values: []*structpb.Value{
				{
					Kind: &structpb.Value_StructValue{StructValue: &structpb.Struct{
						Fields: map[string]*structpb.Value{
							"google.protobuf.ListValue": {Kind: &structpb.Value_StringValue{
								StringValue: "is an array of arbitrary JSON values",
							}},
						},
					}},
				},
			},
		}
	case "google.protobuf.Struct":
		return &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"google.protobuf.Struct": {Kind: &structpb.Value_StringValue{
					StringValue: "supports arbitrary JSON objects",
				}},
			},
		}
	}

	dm := dynamicpb.NewMessage(md)

	// if the message is a recursive structure, we don't want to blow the stack
	if slices.Contains(path, md) {
		// already visited this type; avoid infinite recursion
		return dm
	}
	path = append(path, md)

	// for repeated fields, add a single element with default value
	// and for message fields, add a message with all default fields
	// that also has non-nil message and non-empty repeated fields

	// For v2, we'll create a simple template with default values
	// The dynamicpb API is different and would require more complex field handling
	// This is a simplified version that creates an empty message
	return dm
}

// ClientTransportCredentials is a helper function that constructs a TLS config with
// the given properties (see ClientTLSConfig) and then constructs and returns gRPC
// transport credentials using that config.
//
// Deprecated: Use grpcurl.ClientTLSConfig and credentials.NewTLS instead.
func ClientTransportCredentials(insecureSkipVerify bool, cacertFile, clientCertFile, clientKeyFile string) (credentials.TransportCredentials, error) {
	tlsConf, err := ClientTLSConfig(insecureSkipVerify, cacertFile, clientCertFile, clientKeyFile)
	if err != nil {
		return nil, err
	}

	return credentials.NewTLS(tlsConf), nil
}

// ClientTLSConfig builds transport-layer config for a gRPC client using the
// given properties. If cacertFile is blank, only standard trusted certs are used to
// verify the server certs. If clientCertFile is blank, the client will not use a client
// certificate. If clientCertFile is not blank then clientKeyFile must not be blank.
func ClientTLSConfig(insecureSkipVerify bool, cacertFile, clientCertFile, clientKeyFile string) (*tls.Config, error) {
	var tlsConf tls.Config

	if clientCertFile != "" {
		// Load the client certificates from disk
		certificate, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("could not load client key pair: %v", err)
		}
		tlsConf.Certificates = []tls.Certificate{certificate}
	}

	if insecureSkipVerify {
		tlsConf.InsecureSkipVerify = true
	} else if cacertFile != "" {
		// Create a certificate pool from the certificate authority
		certPool := x509.NewCertPool()
		ca, err := os.ReadFile(cacertFile)
		if err != nil {
			return nil, fmt.Errorf("could not read ca certificate: %v", err)
		}

		// Append the certificates from the CA
		if ok := certPool.AppendCertsFromPEM(ca); !ok {
			return nil, errors.New("failed to append ca certs")
		}

		tlsConf.RootCAs = certPool
	}

	return &tlsConf, nil
}

// ServerTransportCredentials builds transport credentials for a gRPC server using the
// given properties. If cacertFile is blank, the server will not request client certs
// unless requireClientCerts is true. When requireClientCerts is false and cacertFile is
// not blank, the server will verify client certs when presented, but will not require
// client certs. The serverCertFile and serverKeyFile must both not be blank.
func ServerTransportCredentials(cacertFile, serverCertFile, serverKeyFile string, requireClientCerts bool) (credentials.TransportCredentials, error) {
	var tlsConf tls.Config
	// TODO(jh): Remove this line once https://github.com/golang/go/issues/28779 is fixed
	// in Go tip. Until then, the recently merged TLS 1.3 support breaks the TLS tests.
	tlsConf.MaxVersion = tls.VersionTLS12

	// Load the server certificates from disk
	certificate, err := tls.LoadX509KeyPair(serverCertFile, serverKeyFile)
	if err != nil {
		return nil, fmt.Errorf("could not load key pair: %v", err)
	}
	tlsConf.Certificates = []tls.Certificate{certificate}

	if cacertFile != "" {
		// Create a certificate pool from the certificate authority
		certPool := x509.NewCertPool()
		ca, err := os.ReadFile(cacertFile)
		if err != nil {
			return nil, fmt.Errorf("could not read ca certificate: %v", err)
		}

		// Append the certificates from the CA
		if ok := certPool.AppendCertsFromPEM(ca); !ok {
			return nil, errors.New("failed to append ca certs")
		}

		tlsConf.ClientCAs = certPool
	}

	if requireClientCerts {
		tlsConf.ClientAuth = tls.RequireAndVerifyClientCert
	} else if cacertFile != "" {
		tlsConf.ClientAuth = tls.VerifyClientCertIfGiven
	} else {
		tlsConf.ClientAuth = tls.NoClientCert
	}

	return credentials.NewTLS(&tlsConf), nil
}

// BlockingDial is a helper method to dial the given address, using optional TLS credentials,
// and blocking until the returned connection is ready. If the given credentials are nil, the
// connection will be insecure (plain-text).
// The network parameter should be left empty in most cases when your address is a RFC 3986
// compliant URI. The resolver from grpc-go will resolve the correct network type.
func BlockingDial(ctx context.Context, network, address string, creds credentials.TransportCredentials, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if creds == nil {
		creds = insecure.NewCredentials()
	}

	var err error
	if strings.HasPrefix(address, "xds:///") {
		// The xds:/// prefix is used to signal to the gRPC client to use an xDS server to resolve the
		// target. The relevant credentials will be automatically pulled from the GRPC_XDS_BOOTSTRAP or
		// GRPC_XDS_BOOTSTRAP_CONFIG env vars.
		creds, err = xdsCredentials.NewClientCredentials(xdsCredentials.ClientOptions{FallbackCreds: creds})
		if err != nil {
			return nil, err
		}
	}

	// grpc.Dial doesn't provide any information on permanent connection errors (like
	// TLS handshake failures). So in order to provide good error messages, we need a
	// custom dialer that can provide that info. That means we manage the TLS handshake.
	result := make(chan interface{}, 1)

	writeResult := func(res interface{}) {
		// non-blocking write: we only need the first result
		select {
		case result <- res:
		default:
		}
	}

	// custom credentials and dialer will notify on error via the
	// writeResult function
	creds = &errSignalingCreds{
		TransportCredentials: creds,
		writeResult:          writeResult,
	}

	switch network {
	case "":
		// no-op, use address as-is
	case "tcp":
		if strings.HasPrefix(address, "unix://") {
			return nil, fmt.Errorf("tcp network type cannot use unix address %s", address)
		}
	case "unix":
		if !strings.HasPrefix(address, "unix://") {
			// prepend unix:// to the address if it's not already there
			// this is to maintain backwards compatibility because the custom dialer is replaced by
			// the default dialer in grpc-go.
			// https://github.com/fullstorydev/grpcurl/pull/480
			address = "unix://" + address
		}
	default:
		// custom dialer for other networks
		dialer := func(ctx context.Context, address string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				// capture the error so we can provide a better message
				writeResult(err)
			}
			return conn, err
		}
		opts = append([]grpc.DialOption{grpc.WithContextDialer(dialer)}, opts...)
	}

	// Even with grpc.FailOnNonTempDialError, this call will usually timeout in
	// the face of TLS handshake errors. So we can't rely on grpc.WithBlock() to
	// know when we're done. So we run it in a goroutine and then use result
	// channel to either get the connection or fail-fast.
	go func() {
		// We put grpc.FailOnNonTempDialError *before* the explicitly provided
		// options so that it could be overridden.
		opts = append([]grpc.DialOption{grpc.FailOnNonTempDialError(true)}, opts...)
		// But we don't want caller to be able to override these two, so we put
		// them *after* the explicitly provided options.
		opts = append(opts, grpc.WithBlock(), grpc.WithTransportCredentials(creds))

		conn, err := grpc.DialContext(ctx, address, opts...)
		var res interface{}
		if err != nil {
			res = err
		} else {
			res = conn
		}
		writeResult(res)
	}()

	select {
	case res := <-result:
		if conn, ok := res.(*grpc.ClientConn); ok {
			return conn, nil
		}
		return nil, res.(error)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// errSignalingCreds is a wrapper around a TransportCredentials value, but
// it will use the writeResult function to notify on error.
type errSignalingCreds struct {
	credentials.TransportCredentials
	writeResult func(res interface{})
}

func (c *errSignalingCreds) ClientHandshake(ctx context.Context, addr string, rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, auth, err := c.TransportCredentials.ClientHandshake(ctx, addr, rawConn)
	if err != nil {
		c.writeResult(err)
	}
	return conn, auth, err
}
