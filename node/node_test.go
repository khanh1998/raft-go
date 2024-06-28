package node

import (
	"khanh/raft-go/common"
	"khanh/raft-go/http_proxy"
	"khanh/raft-go/logic"
	"khanh/raft-go/rpc_proxy"
	"khanh/raft-go/state_machine"
	"net/rpc"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestRpcConnection(t *testing.T) {
	n := NewNode(NewNodeParams{
		Brain: logic.NewRaftBrainParams{
			ID:   1,
			Mode: common.Dynamic,
			Members: []common.ClusterMember{
				{
					ID:      1,
					RpcUrl:  "localhost:1234",
					HttpUrl: "localhost:8080",
				},
			},
			CachingUp:           false,
			HeartBeatTimeOutMin: 150,
			HeartBeatTimeOutMax: 300,
			ElectionTimeOutMin:  300,
			ElectionTimeOutMax:  500,
			Logger:              &zerolog.Logger{},
			DB:                  common.NewPersistenceMock(),
		},
		RPCProxy: rpc_proxy.NewRPCImplParams{
			HostID:  1,
			HostURL: "localhost:1234",
			Logger:  &zerolog.Logger{},
		},
		HTTPProxy: http_proxy.NewHttpProxyParams{
			URL:    "localhost:8080",
			Logger: &zerolog.Logger{},
		},
		StateMachine: state_machine.NewKeyValueStateMachineParams{
			DB: common.NewPersistenceMock(),
		},
		Logger:     &zerolog.Logger{},
		DataFolder: "data/",
	})

	n.Start(false, false)

	_, err := rpc.Dial("tcp", ":1234")
	assert.NoError(t, err, "connect to rpc server ok")

	n.Stop()

	time.Sleep(3 * time.Second)

	_, err = rpc.Dial("tcp", ":1234")
	assert.Error(t, err, "connect to rpc server not ok")
}
