package rpc_proxy

import (
	"context"
	"fmt"
	"khanh/raft-go/common"

	"go.opentelemetry.io/otel/trace"
)

func (r *RPCProxyImpl) AppendEntries(input *common.AppendEntriesInput, output *common.AppendEntriesOutput) (err error) {
	traceID, err := trace.TraceIDFromHex(input.TraceID)
	if err != nil {
		r.logger.Error("RequestVote_TraceIDFromHex", err)
	}

	spanID, err := trace.SpanIDFromHex(input.SpanID)
	if err != nil {
		r.logger.Error("RequestVote_SpanIDFromHex", err)
	}

	traceFlags := trace.TraceFlags(input.TraceFlags)

	traceState, err := trace.ParseTraceState(input.TraceState)
	if err != nil {
		r.logger.Error("RequestVote_ParseTraceState", err)
	}

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		Remote:     true,
		TraceFlags: traceFlags,
		TraceState: traceState,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	ctx, span := tracer.Start(ctx, "AppendEntriesInvoked")
	defer span.End()

	if !r.accessible {
		return ErrInaccessible
	}
	return r.brain.AppendEntries(ctx, input, output)
}

func (r *RPCProxyImpl) RequestVote(input *common.RequestVoteInput, output *common.RequestVoteOutput) (err error) {
	traceID, err := trace.TraceIDFromHex(input.TraceID)
	if err != nil {
		r.logger.Error("RequestVote_TraceIDFromHex", err)
	}

	spanID, err := trace.SpanIDFromHex(input.SpanID)
	if err != nil {
		r.logger.Error("RequestVote_SpanIDFromHex", err)
	}

	traceFlags := trace.TraceFlags(input.TraceFlags)

	traceState, err := trace.ParseTraceState(input.TraceState)
	if err != nil {
		r.logger.Error("RequestVote_ParseTraceState", err)
	}

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		Remote:     true,
		TraceFlags: traceFlags,
		TraceState: traceState,
	})

	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	ctx, span := tracer.Start(ctx, "RequestVoteInvoked")
	defer span.End()

	if !r.accessible {
		return ErrInaccessible
	}
	return r.brain.RequestVote(ctx, input, output)
}

func (r *RPCProxyImpl) Ping(name string, message *common.PingResponse) (err error) {
	if !r.accessible {
		return ErrInaccessible
	}

	info := r.brain.GetInfo()

	*message = common.PingResponse{
		ID:       r.hostID,
		RpcUrl:   r.hostURL,
		Message:  fmt.Sprintf("Hello %s, from node ID: %d, URL: %s", name, r.hostID, r.hostURL),
		LeaderId: info.LeaderId,
		State:    info.State,
		Term:     info.Term,
	}
	return nil
}

func (r *RPCProxyImpl) GetInfo(_ *struct{}, info *common.GetStatusResponse) (err error) {
	if !r.accessible {
		return ErrInaccessible
	}

	*info = r.brain.GetInfo()

	r.log().Info("received GetInfo request")

	return nil
}

// TODO: add span_id and trace_id into request
func (r *RPCProxyImpl) ToVotingMember(_ *struct{}, _ *struct{}) (err error) {
	ctx, span := tracer.Start(context.Background(), "ToVotingMember")
	defer span.End()

	return r.brain.ToVotingMember(ctx)
}
