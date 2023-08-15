package rpc_proxy

import (
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"time"
)

var ErrInaccessible = errors.New("rpc proxy is in accessible")

func (r RPCProxyImpl) SendAppendEntries(peerId int, timeout *time.Duration, input common.AppendEntriesInput) (output common.AppendEntriesOutput, err error) {
	if !r.Accessible {
		return output, ErrInaccessible
	}

	serviceMethod := "RPCProxyImpl.AppendEntries"

	if timeout != nil {
		if err := r.CallWithTimeout(peerId, serviceMethod, input, &output, *timeout); err != nil {
			return output, nil
		}
	} else {
		if err := r.CallWithoutTimeout(peerId, serviceMethod, input, &output); err != nil {
			return output, nil
		}
	}

	return output, nil
}

func (r RPCProxyImpl) SendRequestVote(peerId int, timeout *time.Duration, input common.RequestVoteInput) (output common.RequestVoteOutput, err error) {
	if !r.Accessible {
		return output, ErrInaccessible
	}
	serviceMethod := "RPCProxyImpl.RequestVote"

	if timeout != nil {
		if err := r.CallWithTimeout(peerId, serviceMethod, input, &output, *timeout); err != nil {
			return output, nil
		}
	} else {
		if err := r.CallWithoutTimeout(peerId, serviceMethod, input, &output); err != nil {
			return output, nil
		}
	}

	return output, nil
}

func (r RPCProxyImpl) SendPing(peerId int, timeout *time.Duration) (err error) {
	if !r.Accessible {
		return ErrInaccessible
	}
	serviceMethod := "RPCProxyImpl.Ping"

	senderName := fmt.Sprintf("hello from Node %d", r.hostID)
	responseMsg := ""

	if timeout != nil {
		if err := r.CallWithTimeout(peerId, serviceMethod, senderName, &responseMsg, *timeout); err != nil {
			return err
		}
	} else {
		if err := r.CallWithoutTimeout(peerId, serviceMethod, senderName, &responseMsg); err != nil {
			return err
		}
	}

	return nil
}
