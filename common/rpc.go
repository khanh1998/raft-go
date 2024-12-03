package common

import (
	"context"
	"net/rpc"

	"go.opentelemetry.io/otel/trace"
)

type PeerRPCProxyConnectInfo struct {
	ID  int
	URL string
}

type PeerRPCProxy struct {
	Conn *rpc.Client
	URL  string
}

type PingRequest struct {
	ID    int
	Trace *RequestTraceInfo
}

type PingResponse struct {
	ID       int
	LeaderId int
	RpcUrl   string
	Message  string
	State    RaftState
	Term     int
}

// this struct carries need info so OpenTelemery can trace the requests between nodes.
type RequestTraceInfo struct {
	TraceID    string
	SpanID     string
	TraceFlags byte
	TraceState string
}

func NewRequestTraceInfo(traceId string, spanId string, flags byte, state string) RequestTraceInfo {
	return RequestTraceInfo{
		TraceID:    traceId,
		SpanID:     spanId,
		TraceFlags: flags,
		TraceState: state,
	}
}

func (r *RequestTraceInfo) Context() (context.Context, error) {
	traceID, err := trace.TraceIDFromHex(r.TraceID)
	if err != nil {
		return nil, err
	}

	spanID, err := trace.SpanIDFromHex(r.SpanID)
	if err != nil {
		return nil, err
	}

	traceFlags := trace.TraceFlags(r.TraceFlags)

	traceState, err := trace.ParseTraceState(r.TraceState)
	if err != nil {
		return nil, err
	}

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		Remote:     true,
		TraceFlags: traceFlags,
		TraceState: traceState,
	})

	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	return ctx, nil
}
