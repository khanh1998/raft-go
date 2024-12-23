package rpc_proxy

import (
	"context"
	"errors"
	gc "khanh/raft-go/common"
	"khanh/raft-go/raft_core/common"
	"time"

	"go.opentelemetry.io/otel/trace"
)

var ErrInaccessible = errors.New("rpc proxy is in accessible")

func extractSpanInfo(span trace.Span) (valid bool, traceID string, spanID string, traceFlags byte, traceState string) {
	valid = span.SpanContext().IsValid()
	traceID = span.SpanContext().TraceID().String()
	spanID = span.SpanContext().SpanID().String()
	traceFlags = byte(span.SpanContext().TraceFlags())
	traceState = span.SpanContext().TraceState().String()
	return
}

func (r *RPCProxyImpl) SendInstallSnapshot(ctx context.Context, peerId int, timeout *time.Duration, input common.InstallSnapshotInput) (output common.InstallSnapshotOutput, err error) {
	if !r.accessible {
		return output, ErrInaccessible
	}

	if r.simulation != nil {
		if err = r.simulation.ProcessInbound(peerId); err != nil {
			r.log().ErrorContext(ctx, "Ping_ProcessInbound", err, "input", input)

			return output, err
		}
	}

	ctx, span := tracer.Start(ctx, "SendInstallSnapshot")
	defer span.End()

	validSpan, traceID, spanID, traceFlags, traceState := extractSpanInfo(span)
	if validSpan {
		input.Trace = &gc.RequestTraceInfo{
			SpanID:     spanID,
			TraceID:    traceID,
			TraceFlags: traceFlags,
			TraceState: traceState,
		}
	}

	serviceMethod := "RPCProxyImpl.InstallSnapshot"

	if timeout != nil {
		if err := r.callWithTimeout(ctx, peerId, serviceMethod, input, &output, *timeout); err != nil {
			return output, err
		}
	} else {
		if err := r.callWithoutTimeout(ctx, peerId, serviceMethod, input, &output); err != nil {
			return output, err
		}
	}

	return output, nil
}

func (r *RPCProxyImpl) SendAppendEntries(ctx context.Context, peerId int, timeout *time.Duration, input common.AppendEntriesInput) (output common.AppendEntriesOutput, err error) {
	begin := time.Now()
	if !r.accessible {
		return output, ErrInaccessible
	}

	if r.simulation != nil {
		if err = r.simulation.ProcessInbound(peerId); err != nil {
			r.log().ErrorContext(ctx, "SendAppendEntries_ProcessInbound", err, "input", input)

			return output, err
		}
	}

	ctx, span := tracer.Start(ctx, "SendAppendEntries")
	defer span.End()

	validSpan, traceID, spanID, traceFlags, traceState := extractSpanInfo(span)
	if validSpan {
		input.Trace = &gc.RequestTraceInfo{
			SpanID:     spanID,
			TraceID:    traceID,
			TraceFlags: traceFlags,
			TraceState: traceState,
		}
	}

	serviceMethod := "RPCProxyImpl.AppendEntries"

	if timeout != nil {
		if err := r.callWithTimeout(ctx, peerId, serviceMethod, input, &output, *timeout); err != nil {
			return output, err
		}
	} else {
		if err := r.callWithoutTimeout(ctx, peerId, serviceMethod, input, &output); err != nil {
			return output, err
		}
	}

	duration := time.Since(begin)
	r.log().InfoContext(ctx, "SendAppendEntries", "duration", duration.String())

	return output, nil
}

func (r *RPCProxyImpl) SendRequestVote(ctx context.Context, peerId int, timeout *time.Duration, input common.RequestVoteInput) (output common.RequestVoteOutput, err error) {
	begin := time.Now()
	if !r.accessible {
		return output, ErrInaccessible
	}

	if r.simulation != nil {
		if err = r.simulation.ProcessInbound(peerId); err != nil {
			r.log().ErrorContext(ctx, "SendRequestVote_ProcessInbound", err, "input", input)

			return output, err
		}
	}

	ctx, span := tracer.Start(ctx, "SendRequestVote")
	defer span.End()

	validSpan, traceID, spanID, traceFlags, traceState := extractSpanInfo(span)
	if validSpan {
		input.Trace = &gc.RequestTraceInfo{
			SpanID:     spanID,
			TraceID:    traceID,
			TraceFlags: traceFlags,
			TraceState: traceState,
		}
	}

	serviceMethod := "RPCProxyImpl.RequestVote"

	if timeout != nil {
		if err := r.callWithTimeout(ctx, peerId, serviceMethod, input, &output, *timeout); err != nil {
			return output, err
		}
	} else {
		if err := r.callWithoutTimeout(ctx, peerId, serviceMethod, input, &output); err != nil {
			return output, err
		}
	}

	duration := time.Since(begin)
	r.log().InfoContext(ctx, "SendRequestVote", "duration", duration.String())

	return output, nil
}

func (r *RPCProxyImpl) SendPing(ctx context.Context, peerId int, timeout *time.Duration) (responseMsg gc.PingResponse, err error) {
	begin := time.Now()
	if !r.accessible {
		return responseMsg, ErrInaccessible
	}

	if r.simulation != nil {
		if err = r.simulation.ProcessInbound(peerId); err != nil {
			r.log().ErrorContext(ctx, "SendPing_ProcessInbound", err)

			return responseMsg, err
		}
	}

	ctx, span := tracer.Start(ctx, "SendAppendEntries")
	defer span.End()

	input := gc.PingRequest{
		ID:    r.hostID,
		Trace: nil,
	}

	validSpan, traceID, spanID, traceFlags, traceState := extractSpanInfo(span)
	if validSpan {
		input.Trace = &gc.RequestTraceInfo{
			SpanID:     spanID,
			TraceID:    traceID,
			TraceFlags: traceFlags,
			TraceState: traceState,
		}
	}

	serviceMethod := "RPCProxyImpl.Ping"

	if timeout != nil {
		if err := r.callWithTimeout(ctx, peerId, serviceMethod, input, &responseMsg, *timeout); err != nil {
			return responseMsg, err
		}
	} else {
		if err := r.callWithoutTimeout(ctx, peerId, serviceMethod, input, &responseMsg); err != nil {
			return responseMsg, err
		}
	}
	duration := time.Since(begin)
	r.log().InfoContext(ctx, "SendPing", "duration", duration.String())

	return responseMsg, nil
}

func (r *RPCProxyImpl) SendToVotingMember(ctx context.Context, peerId int, timeout *time.Duration) (err error) {
	if !r.accessible {
		return ErrInaccessible
	}

	if r.simulation != nil {
		if err = r.simulation.ProcessInbound(peerId); err != nil {
			r.log().ErrorContext(ctx, "SendToVotingMember_ProcessInbound", err)

			return err
		}
	}

	serviceMethod := "RPCProxyImpl.ToVotingMember"
	input, output := struct{}{}, struct{}{}

	if timeout != nil {
		if err := r.callWithTimeout(ctx, peerId, serviceMethod, input, &output, *timeout); err != nil {
			return err
		}
	} else {
		if err := r.callWithoutTimeout(ctx, peerId, serviceMethod, input, &output); err != nil {
			return err
		}
	}

	return nil
}
