package rpc_proxy

import (
	"context"
	"fmt"
	gc "khanh/raft-go/common"
	"khanh/raft-go/raft_core/common"

	"go.opentelemetry.io/otel/trace"
)

func (r *RPCProxyImpl) InstallSnapshot(input *common.InstallSnapshotInput, output *common.InstallSnapshotOutput) (err error) {
	if !r.accessible {
		return ErrInaccessible
	}

	ctx := context.Background()

	if r.simulation != nil {
		if err = r.simulation.ProcessInbound(input.LeaderId); err != nil {
			r.log().ErrorContext(ctx, "InstallSnapshot_ProcessInbound", err, "input", input)

			return err
		}
	}

	if input.Trace != nil {
		var span trace.Span
		ctx, err = input.Trace.Context()
		if err != nil {
			r.log().ErrorContext(ctx, "InstallSnapshot: get context", err)
		}

		ctx, span = tracer.Start(ctx, "InstallSnapshotInvoked")
		defer span.End()
	}

	r.brain.InstallSnapshot(ctx, input, output)

	return nil
}

func (r *RPCProxyImpl) AppendEntries(input *common.AppendEntriesInput, output *common.AppendEntriesOutput) (err error) {
	if !r.accessible {
		return ErrInaccessible
	}

	ctx := context.Background()

	if r.simulation != nil {
		if err = r.simulation.ProcessInbound(input.LeaderID); err != nil {
			r.log().ErrorContext(ctx, "AppendEntries_ProcessInbound", err, "input", input)

			return err
		}
	}

	if input.Trace != nil {
		var span trace.Span
		ctx, err = input.Trace.Context()
		if err != nil {
			r.log().ErrorContext(ctx, "AppendEntries: get context", err)
		}

		ctx, span = tracer.Start(ctx, "AppendEntriesInvoked")
		defer span.End()
	}

	return r.brain.AppendEntries(ctx, input, output)
}

func (r *RPCProxyImpl) RequestVote(input *common.RequestVoteInput, output *common.RequestVoteOutput) (err error) {
	if !r.accessible {
		return ErrInaccessible
	}

	ctx := context.Background()

	if r.simulation != nil {
		if err = r.simulation.ProcessInbound(input.CandidateID); err != nil {
			r.log().ErrorContext(ctx, "RequestVote_ProcessInbound", err, "input", input)

			return err
		}
	}

	if input.Trace != nil {
		var span trace.Span
		ctx, err = input.Trace.Context()
		if err != nil {
			r.log().ErrorContext(ctx, "RequestVote: get context", err)
		}

		ctx, span = tracer.Start(ctx, "RequestVoteInvoked")
		defer span.End()
	}

	return r.brain.RequestVote(ctx, input, output)
}

func (r *RPCProxyImpl) Ping(input *gc.PingRequest, message *gc.PingResponse) (err error) {
	if !r.accessible {
		return ErrInaccessible
	}

	ctx := context.Background()

	if r.simulation != nil {
		if err = r.simulation.ProcessInbound(input.ID); err != nil {
			r.log().ErrorContext(ctx, "Ping_ProcessInbound", err, "input", input)

			return err
		}
	}

	if input.Trace != nil {
		var span trace.Span
		ctx, err = input.Trace.Context()
		if err != nil {
			r.log().ErrorContext(ctx, "Ping: get context", err)
		}

		ctx, span = tracer.Start(ctx, "PingInvoked")
		defer span.End()
	}

	defer func() {
		if input.ID > 0 {
			peer, err := r.getPeer(input.ID)
			if err == nil && peer.Conn == nil {
				err := r.connectToPeer(ctx, input.ID, 1, r.rpcDialTimeout)
				if err != nil {
					r.log().ErrorContext(
						ctx, "Ping_ConnectToPeer", err,
						"input", input,
					)
				}
			}
		}
	}()

	info := r.brain.GetInfo()

	*message = gc.PingResponse{
		ID:       r.hostID,
		RpcUrl:   r.hostURL,
		Message:  fmt.Sprintf("Hello %d, from node ID: %d, URL: %s", input.ID, r.hostID, r.hostURL),
		LeaderId: info.LeaderId,
		State:    info.State,
		Term:     info.Term,
	}
	return nil
}

func (r *RPCProxyImpl) GetInfo(req *gc.GetStatusRequest, info *gc.GetStatusResponse) (err error) {
	if !r.accessible {
		return ErrInaccessible
	}

	ctx := context.Background()

	if r.simulation != nil {
		if err = r.simulation.ProcessInbound(req.SenderId); err != nil {
			r.log().ErrorContext(ctx, "Ping_ProcessInbound", err, "input", req)

			return err
		}
	}

	*info = r.brain.GetInfo()

	r.log().Info("received GetInfo request")

	return nil
}

// TODO: add span_id and trace_id into request
func (r *RPCProxyImpl) ToVotingMember(_ *struct{}, _ *struct{}) (err error) {
	// considering to add restricting logic,
	// it is unnecessary
	if !r.accessible {
		return ErrInaccessible
	}

	ctx, span := tracer.Start(context.Background(), "ToVotingMember")
	defer span.End()

	return r.brain.ToVotingMember(ctx)
}
