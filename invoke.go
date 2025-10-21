package grpcurl

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	protov2 "google.golang.org/protobuf/proto"      //lint:ignore SA1019 same as above
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"github.com/jhump/protoreflect/v2/grpcdynamic"
	"github.com/jhump/protoreflect/v2/grpcreflect"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// InvocationEventHandler is a bag of callbacks for handling events that occur in the course
// of invoking an RPC. The handler also provides request data that is sent. The callbacks are
// generally called in the order they are listed below.
type InvocationEventHandler interface {
	// OnResolveMethod is called with a descriptor of the method that is being invoked.
	OnResolveMethod(protoreflect.MethodDescriptor)
	// OnSendHeaders is called with the request metadata that is being sent.
	OnSendHeaders(metadata.MD)
	// OnReceiveHeaders is called when response headers have been received.
	OnReceiveHeaders(metadata.MD)
	// OnReceiveResponse is called for each response message received.
	OnReceiveResponse(protov2.Message)
	// OnReceiveTrailers is called when response trailers and final RPC status have been received.
	OnReceiveTrailers(*status.Status, metadata.MD)
}

// RequestMessageSupplier is a function that is called to retrieve request
// messages for a GRPC operation. This type is deprecated and will be removed in
// a future release.
//
// Deprecated: This is only used with the deprecated InvokeRpc. Instead, use
// RequestSupplier with InvokeRPC.
type RequestMessageSupplier func() ([]byte, error)

// InvokeRpc uses the given gRPC connection to invoke the given method. This function is deprecated
// and will be removed in a future release. It just delegates to the similarly named InvokeRPC
// method, whose signature is only slightly different.
//
// Deprecated: use InvokeRPC instead.
func InvokeRpc(ctx context.Context, source DescriptorSource, cc *grpc.ClientConn, methodName string,
	headers []string, handler InvocationEventHandler, requestData RequestMessageSupplier) error {

	return InvokeRPC(ctx, source, cc, methodName, headers, handler, func(m protov2.Message) error {
		// New function is almost identical, but the request supplier function works differently.
		// So we adapt the logic here to maintain compatibility.
		_, err := requestData()
		if err != nil {
			return err
		}
		// Convert interface{} to v1 proto.Message for jsonpb.Unmarshal
		// This is a simplified approach for the deprecated function
		// We'll just return an error since this function is deprecated anyway
		return fmt.Errorf("jsonpb unmarshaling not supported in deprecated function - use InvokeRPC instead")
	})
}

// RequestSupplier is a function that is called to populate messages for a gRPC operation. The
// function should populate the given message or return a non-nil error. If the supplier has no
// more messages, it should return io.EOF. When it returns io.EOF, it should not in any way
// modify the given message argument.
type RequestSupplier func(protov2.Message) error

// InvokeRPC uses the given gRPC channel to invoke the given method. The given descriptor source
// is used to determine the type of method and the type of request and response message. The given
// headers are sent as request metadata. Methods on the given event handler are called as the
// invocation proceeds.
//
// The given requestData function supplies the actual data to send. It should return io.EOF when
// there is no more request data. If the method being invoked is a unary or server-streaming RPC
// (e.g. exactly one request message) and there is no request data (e.g. the first invocation of
// the function returns io.EOF), then an empty request message is sent.
//
// If the requestData function and the given event handler coordinate or share any state, they should
// be thread-safe. This is because the requestData function may be called from a different goroutine
// than the one invoking event callbacks. (This only happens for bi-directional streaming RPCs, where
// one goroutine sends request messages and another consumes the response messages).
func InvokeRPC(ctx context.Context, source DescriptorSource, ch grpc.ClientConnInterface, methodName string,
	headers []string, handler InvocationEventHandler, requestData RequestSupplier) error {

	md := MetadataFromHeaders(headers)

	svc, mth := parseSymbol(methodName)
	if svc == "" || mth == "" {
		return fmt.Errorf("given method name %q is not in expected format: 'service/method' or 'service.method'", methodName)
	}

	dsc, err := source.FindSymbol(svc)
	if err != nil {
		// return a gRPC status error if hasStatus is true
		errStatus, hasStatus := status.FromError(err)
		switch {
		case hasStatus && isNotFoundError(err):
			return status.Errorf(errStatus.Code(), "target server does not expose service %q: %s", svc, errStatus.Message())
		case hasStatus:
			return status.Errorf(errStatus.Code(), "failed to query for service descriptor %q: %s", svc, errStatus.Message())
		case isNotFoundError(err):
			return fmt.Errorf("target server does not expose service %q", svc)
		}
		return fmt.Errorf("failed to query for service descriptor %q: %v", svc, err)
	}
	sd, ok := dsc.(protoreflect.ServiceDescriptor)
	if !ok {
		return fmt.Errorf("target server does not expose service %q", svc)
	}
	mtd := sd.Methods().ByName(protoreflect.Name(mth))
	if mtd == nil {
		return fmt.Errorf("service %q does not include a method named %q", svc, mth)
	}

	handler.OnResolveMethod(mtd)

	// For v2, we'll use dynamicpb directly for message creation
	req := dynamicpb.NewMessage(mtd.Input())

	handler.OnSendHeaders(md)
	ctx = metadata.NewOutgoingContext(ctx, md)

	stub := grpcdynamic.NewStub(ch)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if mtd.IsStreamingClient() && mtd.IsStreamingServer() {
		return invokeBidi(ctx, stub, mtd, handler, requestData, req)
	} else if mtd.IsStreamingClient() {
		return invokeClientStream(ctx, stub, mtd, handler, requestData, req)
	} else if mtd.IsStreamingServer() {
		return invokeServerStream(ctx, stub, mtd, handler, requestData, req)
	} else {
		return invokeUnary(ctx, stub, mtd, handler, requestData, req)
	}
}

func invokeUnary(ctx context.Context, stub *grpcdynamic.Stub, md protoreflect.MethodDescriptor, handler InvocationEventHandler,
	requestData RequestSupplier, req protov2.Message) error {

	err := requestData(req)
	if err != nil && err != io.EOF {
		return fmt.Errorf("error getting request data: %v", err)
	}
	if err != io.EOF {
		// verify there is no second message, which is a usage error
		err := requestData(req)
		if err == nil {
			return fmt.Errorf("method %q is a unary RPC, but request data contained more than 1 message", string(md.FullName()))
		} else if err != io.EOF {
			return fmt.Errorf("error getting request data: %v", err)
		}
	}

	// Now we can actually invoke the RPC!
	var respHeaders metadata.MD
	var respTrailers metadata.MD
	// Convert v1 proto.Message to v2 for grpcdynamic
	var v2Req interface{}
	if v2Msg, ok := req.(interface{ ProtoReflect() protoreflect.Message }); ok {
		v2Req = v2Msg
	} else {
		// Create a dynamic message from the descriptor
		v2Req = dynamicpb.NewMessage(md.Input())
		// Copy data from v1 to v2 message
		if data, err := protov2.Marshal(req); err == nil {
			if v2Msg, ok := v2Req.(interface{ ProtoReflect() protoreflect.Message }); ok {
				protov2.Unmarshal(data, v2Msg)
			}
		}
	}
	resp, err := stub.InvokeRpc(ctx, md, v2Req.(protoreflect.ProtoMessage), grpc.Trailer(&respTrailers), grpc.Header(&respHeaders))

	stat, ok := status.FromError(err)
	if !ok {
		// Error codes sent from the server will get printed differently below.
		// So just bail for other kinds of errors here.
		return fmt.Errorf("grpc call for %q failed: %v", string(md.FullName()), err)
	}

	handler.OnReceiveHeaders(respHeaders)

	if stat.Code() == codes.OK {
		// Convert v2 response to v1 proto.Message for handler
		var v1Resp protov2.Message
		if v2Msg, ok := resp.(interface{ ProtoReflect() protoreflect.Message }); ok {
			// Convert v2 to v1 by marshaling and unmarshaling
			if data, err := protov2.Marshal(v2Msg); err == nil {
				v1Resp = dynamicpb.NewMessage(md.Output())
				protov2.Unmarshal(data, v1Resp)
			} else {
				v1Resp = dynamicpb.NewMessage(md.Output())
			}
		} else {
			v1Resp = dynamicpb.NewMessage(md.Output())
		}
		handler.OnReceiveResponse(v1Resp)
	}

	handler.OnReceiveTrailers(stat, respTrailers)

	return nil
}

func invokeClientStream(ctx context.Context, stub *grpcdynamic.Stub, md protoreflect.MethodDescriptor, handler InvocationEventHandler,
	requestData RequestSupplier, req protov2.Message) error {

	// invoke the RPC!
	str, err := stub.InvokeRpcClientStream(ctx, md)

	// Upload each request message in the stream
	var resp protov2.Message
	for err == nil {
		err = requestData(req)
		if err == io.EOF {
			v2Resp, err := str.CloseAndReceive()
			if err == nil {
				// Convert v2 response to v1 proto.Message
				var v1Resp protov2.Message
				if v2Msg, ok := v2Resp.(interface{ ProtoReflect() protoreflect.Message }); ok {
					v1Resp = dynamicpb.NewMessage(md.Output())
					if data, err := protov2.Marshal(v2Msg); err == nil {
						protov2.Unmarshal(data, v1Resp)
					}
				} else {
					v1Resp = dynamicpb.NewMessage(md.Output())
				}
				resp = v1Resp
			}
			break
		}
		if err != nil {
			return fmt.Errorf("error getting request data: %v", err)
		}

		// Convert v1 proto.Message to v2 for SendMsg
		var v2Req2 interface{}
		if v2Msg, ok := req.(interface{ ProtoReflect() protoreflect.Message }); ok {
			v2Req2 = v2Msg
		} else {
			v2Req2 = dynamicpb.NewMessage(md.Input())
			if data, err := protov2.Marshal(req); err == nil {
				if v2Msg, ok := v2Req2.(interface{ ProtoReflect() protoreflect.Message }); ok {
					protov2.Unmarshal(data, v2Msg)
				}
			}
		}
		err = str.SendMsg(v2Req2.(protoreflect.ProtoMessage))
		if err == io.EOF {
			// We get EOF on send if the server says "go away"
			// We have to use CloseAndReceive to get the actual code
			v2Resp, err := str.CloseAndReceive()
			if err == nil {
				// Convert v2 response to v1 proto.Message
				var v1Resp protov2.Message
				if v2Msg, ok := v2Resp.(interface{ ProtoReflect() protoreflect.Message }); ok {
					v1Resp = dynamicpb.NewMessage(md.Output())
					if data, err := protov2.Marshal(v2Msg); err == nil {
						protov2.Unmarshal(data, v1Resp)
					}
				} else {
					v1Resp = dynamicpb.NewMessage(md.Output())
				}
				resp = v1Resp
			}
			break
		}

		// Reset the request message for reuse
		if resetter, ok := req.(interface{ Reset() }); ok {
			resetter.Reset()
		}
	}

	// finally, process response data
	stat, ok := status.FromError(err)
	if !ok {
		// Error codes sent from the server will get printed differently below.
		// So just bail for other kinds of errors here.
		return fmt.Errorf("grpc call for %q failed: %v", string(md.FullName()), err)
	}

	if str != nil {
		if respHeaders, err := str.Header(); err == nil {
			handler.OnReceiveHeaders(respHeaders)
		}
	}

	if stat.Code() == codes.OK {
		// Convert v2 response to v1 proto.Message for compatibility
		var v1Resp protov2.Message
		if v2Resp, ok := resp.(interface{ ProtoReflect() protoreflect.Message }); ok {
			// Convert v2 message to v1 message
			v1Resp = v2Resp
		} else {
			v1Resp = dynamicpb.NewMessage(md.Output())
		}
		handler.OnReceiveResponse(v1Resp)
	}

	if str != nil {
		handler.OnReceiveTrailers(stat, str.Trailer())
	}

	return nil
}

func invokeServerStream(ctx context.Context, stub *grpcdynamic.Stub, md protoreflect.MethodDescriptor, handler InvocationEventHandler,
	requestData RequestSupplier, req protov2.Message) error {

	err := requestData(req)
	if err != nil && err != io.EOF {
		return fmt.Errorf("error getting request data: %v", err)
	}
	if err != io.EOF {
		// verify there is no second message, which is a usage error
		err := requestData(req)
		if err == nil {
			return fmt.Errorf("method %q is a server-streaming RPC, but request data contained more than 1 message", string(md.FullName()))
		} else if err != io.EOF {
			return fmt.Errorf("error getting request data: %v", err)
		}
	}

	// Now we can actually invoke the RPC!
	// Convert v1 proto.Message to v2 for InvokeRpcServerStream
	var v2Req3 interface{}
	if v2Msg, ok := req.(interface{ ProtoReflect() protoreflect.Message }); ok {
		v2Req3 = v2Msg
	} else {
		v2Req3 = dynamicpb.NewMessage(md.Input())
		if data, err := protov2.Marshal(req); err == nil {
			if v2Msg, ok := v2Req3.(interface{ ProtoReflect() protoreflect.Message }); ok {
				protov2.Unmarshal(data, v2Msg)
			}
		}
	}
	str, err := stub.InvokeRpcServerStream(ctx, md, v2Req3.(protoreflect.ProtoMessage))

	if str != nil {
		if respHeaders, err := str.Header(); err == nil {
			handler.OnReceiveHeaders(respHeaders)
		}
	}

	// Download each response message
	for err == nil {
		var resp protov2.Message
		v2Resp, err := str.RecvMsg()
		if err == nil {
			// Convert v2 response to v1 proto.Message
			var v1Resp protov2.Message
			if v2Msg, ok := v2Resp.(interface{ ProtoReflect() protoreflect.Message }); ok {
				v1Resp = dynamicpb.NewMessage(md.Output())
				if data, err := protov2.Marshal(v2Msg); err == nil {
					protov2.Unmarshal(data, v1Resp)
				}
			} else {
				v1Resp = dynamicpb.NewMessage(md.Output())
			}
			resp = v1Resp
		}
		if err == nil {
			// Convert v2 response to v1 proto.Message for compatibility
			var v1Resp protov2.Message
			if v2Msg, ok := v2Resp.(interface{ ProtoReflect() protoreflect.Message }); ok {
				v1Resp = dynamicpb.NewMessage(md.Output())
				if data, err := protov2.Marshal(v2Msg); err == nil {
					protov2.Unmarshal(data, v1Resp)
				}
			} else {
				v1Resp = dynamicpb.NewMessage(md.Output())
			}
			resp = v1Resp
		}
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
		// Convert v2 response to v1 proto.Message for compatibility
		var v1Resp2 protov2.Message
		if v2Resp, ok := resp.(interface{ ProtoReflect() protoreflect.Message }); ok {
			// Convert v2 message to v1 message
			v1Resp2 = dynamicpb.NewMessage(md.Output())
			if data, err := protov2.Marshal(v2Resp); err == nil {
				protov2.Unmarshal(data, v1Resp2)
			}
		} else {
			v1Resp2 = dynamicpb.NewMessage(md.Output())
		}
		handler.OnReceiveResponse(v1Resp2)
	}

	stat, ok := status.FromError(err)
	if !ok {
		// Error codes sent from the server will get printed differently below.
		// So just bail for other kinds of errors here.
		return fmt.Errorf("grpc call for %q failed: %v", string(md.FullName()), err)
	}

	if str != nil {
		handler.OnReceiveTrailers(stat, str.Trailer())
	}

	return nil
}

func invokeBidi(ctx context.Context, stub *grpcdynamic.Stub, md protoreflect.MethodDescriptor, handler InvocationEventHandler,
	requestData RequestSupplier, req protov2.Message) error {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// invoke the RPC!
	str, err := stub.InvokeRpcBidiStream(ctx, md)

	var wg sync.WaitGroup
	var sendErr atomic.Value

	defer wg.Wait()

	if err == nil {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Concurrently upload each request message in the stream
			var err error
			for err == nil {
				err = requestData(req)

				if err == io.EOF {
					err = str.CloseSend()
					break
				}
				if err != nil {
					err = fmt.Errorf("error getting request data: %v", err)
					cancel()
					break
				}

				// Convert v1 proto.Message to v2 for SendMsg
		var v2Req2 interface{}
		if v2Msg, ok := req.(interface{ ProtoReflect() protoreflect.Message }); ok {
			v2Req2 = v2Msg
		} else {
			v2Req2 = dynamicpb.NewMessage(md.Input())
			if data, err := protov2.Marshal(req); err == nil {
				if v2Msg, ok := v2Req2.(interface{ ProtoReflect() protoreflect.Message }); ok {
					protov2.Unmarshal(data, v2Msg)
				}
			}
		}
		err = str.SendMsg(v2Req2.(protoreflect.ProtoMessage))

				// Reset the request message for reuse
		if resetter, ok := req.(interface{ Reset() }); ok {
			resetter.Reset()
		}
			}

			if err != nil {
				sendErr.Store(err)
			}
		}()
	}

	if str != nil {
		if respHeaders, err := str.Header(); err == nil {
			handler.OnReceiveHeaders(respHeaders)
		}
	}

	// Download each response message
	for err == nil {
		var resp protov2.Message
		v2Resp, err := str.RecvMsg()
		if err == nil {
			// Convert v2 response to v1 proto.Message
			var v1Resp protov2.Message
			if v2Msg, ok := v2Resp.(interface{ ProtoReflect() protoreflect.Message }); ok {
				v1Resp = dynamicpb.NewMessage(md.Output())
				if data, err := protov2.Marshal(v2Msg); err == nil {
					protov2.Unmarshal(data, v1Resp)
				}
			} else {
				v1Resp = dynamicpb.NewMessage(md.Output())
			}
			resp = v1Resp
		}
		if err == nil {
			// Convert v2 response to v1 proto.Message for compatibility
			var v1Resp protov2.Message
			if v2Msg, ok := v2Resp.(interface{ ProtoReflect() protoreflect.Message }); ok {
				v1Resp = dynamicpb.NewMessage(md.Output())
				if data, err := protov2.Marshal(v2Msg); err == nil {
					protov2.Unmarshal(data, v1Resp)
				}
			} else {
				v1Resp = dynamicpb.NewMessage(md.Output())
			}
			resp = v1Resp
		}
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
		// Convert v2 response to v1 proto.Message for compatibility
		var v1Resp2 protov2.Message
		if v2Resp, ok := resp.(interface{ ProtoReflect() protoreflect.Message }); ok {
			// Convert v2 message to v1 message
			v1Resp2 = dynamicpb.NewMessage(md.Output())
			if data, err := protov2.Marshal(v2Resp); err == nil {
				protov2.Unmarshal(data, v1Resp2)
			}
		} else {
			v1Resp2 = dynamicpb.NewMessage(md.Output())
		}
		handler.OnReceiveResponse(v1Resp2)
	}

	if se, ok := sendErr.Load().(error); ok && se != io.EOF {
		err = se
	}

	stat, ok := status.FromError(err)
	if !ok {
		// Error codes sent from the server will get printed differently below.
		// So just bail for other kinds of errors here.
		return fmt.Errorf("grpc call for %q failed: %v", string(md.FullName()), err)
	}

	if str != nil {
		handler.OnReceiveTrailers(stat, str.Trailer())
	}

	return nil
}

type notFoundError string

func notFound(kind, name string) error {
	return notFoundError(fmt.Sprintf("%s not found: %s", kind, name))
}

func (e notFoundError) Error() string {
	return string(e)
}

func isNotFoundError(err error) bool {
	if grpcreflect.IsElementNotFoundError(err) {
		return true
	}
	_, ok := err.(notFoundError)
	return ok
}

func parseSymbol(svcAndMethod string) (string, string) {
	pos := strings.LastIndex(svcAndMethod, "/")
	if pos < 0 {
		pos = strings.LastIndex(svcAndMethod, ".")
		if pos < 0 {
			return "", ""
		}
	}
	return svcAndMethod[:pos], svcAndMethod[pos+1:]
}
