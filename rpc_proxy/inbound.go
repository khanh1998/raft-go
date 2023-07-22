package rpc_proxy

import (
	"fmt"
	"khanh/raft-go/common"
)

func (r *RPCProxyImpl) AppendEntries(input *common.AppendEntriesInput, output *common.AppendEntriesOutput) (err error) {
	return r.brain.AppendEntries(input, output)
}

func (r *RPCProxyImpl) RequestVote(input *common.RequestVoteInput, output *common.RequestVoteOutput) (err error) {
	return r.brain.RequestVote(input, output)
}

func (r *RPCProxyImpl) Ping(name string, message *string) (err error) {
	*message = fmt.Sprintf("Hello %s, from node ID: %d, URL: %s", name, r.hostID, r.hostURL)
	return nil
}
