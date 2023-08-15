package rpc_proxy

import (
	"fmt"
	"khanh/raft-go/common"
)

func (r *RPCProxyImpl) AppendEntries(input *common.AppendEntriesInput, output *common.AppendEntriesOutput) (err error) {
	if !r.Accessible {
		// TODO: make this timeout
		return ErrInaccessible
	}
	return r.brain.AppendEntries(input, output)
}

func (r *RPCProxyImpl) RequestVote(input *common.RequestVoteInput, output *common.RequestVoteOutput) (err error) {
	if !r.Accessible {
		return ErrInaccessible
	}
	return r.brain.RequestVote(input, output)
}

func (r *RPCProxyImpl) Ping(name string, message *string) (err error) {
	if !r.Accessible {
		return ErrInaccessible
	}
	*message = fmt.Sprintf("Hello %s, from node ID: %d, URL: %s", name, r.hostID, r.hostURL)
	return nil
}
