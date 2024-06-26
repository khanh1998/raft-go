package rpc_proxy

import (
	"fmt"
	"khanh/raft-go/common"
)

func (r *RPCProxyImpl) AppendEntries(input *common.AppendEntriesInput, output *common.AppendEntriesOutput) (err error) {
	if !r.accessible {
		return ErrInaccessible
	}
	return r.brain.AppendEntries(input, output)
}

func (r *RPCProxyImpl) RequestVote(input *common.RequestVoteInput, output *common.RequestVoteOutput) (err error) {
	if !r.accessible {
		return ErrInaccessible
	}
	return r.brain.RequestVote(input, output)
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

	r.log().Info().Msg("received GetInfo request")

	return nil
}

func (r *RPCProxyImpl) ToVotingMember(_ *struct{}, _ *struct{}) (err error) {
	return r.brain.ToVotingMember()
}
