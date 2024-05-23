package rpc_proxy

import (
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"time"
)

var ErrInaccessible = errors.New("rpc proxy is in accessible")

func (r *RPCProxyImpl) SendAppendEntries(peerId int, timeout *time.Duration, input common.AppendEntriesInput) (output common.AppendEntriesOutput, err error) {
	if !r.Accessible {
		return output, ErrInaccessible
	}

	serviceMethod := "RPCProxyImpl.AppendEntries"

	if timeout != nil {
		if err := r.callWithTimeout(peerId, serviceMethod, input, &output, *timeout); err != nil {
			return output, err
		}
	} else {
		if err := r.callWithoutTimeout(peerId, serviceMethod, input, &output); err != nil {
			return output, err
		}
	}

	return output, nil
}

func (r *RPCProxyImpl) SendRequestVote(peerId int, timeout *time.Duration, input common.RequestVoteInput) (output common.RequestVoteOutput, err error) {
	if !r.Accessible {
		return output, ErrInaccessible
	}
	serviceMethod := "RPCProxyImpl.RequestVote"

	if timeout != nil {
		if err := r.callWithTimeout(peerId, serviceMethod, input, &output, *timeout); err != nil {
			return output, err
		}
	} else {
		if err := r.callWithoutTimeout(peerId, serviceMethod, input, &output); err != nil {
			return output, err
		}
	}

	return output, nil
}

func (r *RPCProxyImpl) SendPing(peerId int, timeout *time.Duration) (responseMsg common.PingResponse, err error) {
	if !r.Accessible {
		return responseMsg, ErrInaccessible
	}
	serviceMethod := "RPCProxyImpl.Ping"

	senderName := fmt.Sprintf("hello from Node %d", r.hostID)

	if timeout != nil {
		if err := r.callWithTimeout(peerId, serviceMethod, senderName, &responseMsg, *timeout); err != nil {
			return responseMsg, err
		}
	} else {
		if err := r.callWithoutTimeout(peerId, serviceMethod, senderName, &responseMsg); err != nil {
			return responseMsg, err
		}
	}

	return responseMsg, nil
}

func (r *RPCProxyImpl) SendToVotingMember(peerId int, timeout *time.Duration) (err error) {
	if !r.Accessible {
		return ErrInaccessible
	}
	serviceMethod := "RPCProxyImpl.ToVotingMember"
	input, output := struct{}{}, struct{}{}

	if timeout != nil {
		if err := r.callWithTimeout(peerId, serviceMethod, input, &output, *timeout); err != nil {
			return err
		}
	} else {
		if err := r.callWithoutTimeout(peerId, serviceMethod, input, &output); err != nil {
			return err
		}
	}

	return nil
}
