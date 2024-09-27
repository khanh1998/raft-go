package rpc_proxy

import (
	"context"
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"time"
)

var ErrInaccessible = errors.New("rpc proxy is in accessible")

func (r *RPCProxyImpl) SendAppendEntries(ctx context.Context, peerId int, timeout *time.Duration, input common.AppendEntriesInput) (output common.AppendEntriesOutput, err error) {
	ctx, span := tracer.Start(ctx, "SendAppendEntries")
	defer span.End()

	sc := span.SpanContext()
	traceID, spanID, traceFlags, traceState := sc.TraceID().String(), sc.SpanID().String(), byte(sc.TraceFlags()), sc.TraceState().String()

	input.TraceID = traceID
	input.SpanID = spanID
	input.TraceFlags = traceFlags
	input.TraceState = traceState

	if !r.accessible {
		return output, ErrInaccessible
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

	return output, nil
}

func (r *RPCProxyImpl) SendRequestVote(ctx context.Context, peerId int, timeout *time.Duration, input common.RequestVoteInput) (output common.RequestVoteOutput, err error) {
	ctx, span := tracer.Start(ctx, "SendRequestVote")
	defer span.End()

	sc := span.SpanContext()
	traceID, spanID, traceFlags, traceState := sc.TraceID().String(), sc.SpanID().String(), byte(sc.TraceFlags()), sc.TraceState().String()

	input.TraceID = traceID
	input.SpanID = spanID
	input.TraceFlags = traceFlags
	input.TraceState = traceState

	if !r.accessible {
		return output, ErrInaccessible
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

	return output, nil
}

func (r *RPCProxyImpl) SendPing(ctx context.Context, peerId int, timeout *time.Duration) (responseMsg common.PingResponse, err error) {
	ctx, span := tracer.Start(ctx, "SendAppendEntries")
	defer span.End()

	// todo: include trace into request
	// traceID := span.SpanContext().TraceID().String()
	// spanID := span.SpanContext().SpanID().String()

	// input.TraceID = traceID
	// input.SpanID = spanID

	if !r.accessible {
		return responseMsg, ErrInaccessible
	}
	serviceMethod := "RPCProxyImpl.Ping"

	senderName := fmt.Sprintf("hello from Node %d", r.hostID)

	if timeout != nil {
		if err := r.callWithTimeout(ctx, peerId, serviceMethod, senderName, &responseMsg, *timeout); err != nil {
			return responseMsg, err
		}
	} else {
		if err := r.callWithoutTimeout(ctx, peerId, serviceMethod, senderName, &responseMsg); err != nil {
			return responseMsg, err
		}
	}

	return responseMsg, nil
}

func (r *RPCProxyImpl) SendToVotingMember(ctx context.Context, peerId int, timeout *time.Duration) (err error) {
	if !r.accessible {
		return ErrInaccessible
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
