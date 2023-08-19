package node

import (
	"khanh/raft-go/common"
	"khanh/raft-go/http_proxy"
	"khanh/raft-go/logic"
	"khanh/raft-go/persistance"
	"khanh/raft-go/rpc_proxy"
	"net/rpc"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestStop(t *testing.T) {
	peers := []common.PeerInfo{}

	n := NewNode(NewNodeParams{
		Brain: logic.NewRaftBrainParams{
			ID:                1,
			Peers:             peers,
			DataFileName:      "logs.dat",
			MinRandomDuration: 100,
			MaxRandomDuration: 200,
			Log:               &zerolog.Logger{},
			DB:                persistance.NewPersistenceMock(),
			StateMachine:      common.NewKeyValueStateMachine(),
		},
		RPCProxy: rpc_proxy.NewRPCImplParams{
			Peers:   peers,
			HostID:  1,
			HostURL: ":1234",
			Log:     &zerolog.Logger{},
		},
		HTTPProxy: http_proxy.NewHttpProxyParams{
			URL: "localhost:8080",
		},
	})

	_, err := rpc.Dial("tcp", ":1234")
	assert.NoError(t, err, "connect to rpc server ok")

	n.Stop()

	_, err = rpc.Dial("tcp", ":1234")
	assert.Error(t, err, "connect to rpc server not ok")
}
