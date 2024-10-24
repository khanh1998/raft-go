package node

import (
	"context"
	"khanh/raft-go/common"
	"khanh/raft-go/http_proxy"
	"khanh/raft-go/logic"
	"khanh/raft-go/observability"
	"khanh/raft-go/rpc_proxy"
	"khanh/raft-go/state_machine"
	"net/rpc"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRpcConnection(t *testing.T) {
	logger := observability.NewZerolog(common.ObservabilityConfig{}, 1)
	ctx := context.Background()

	n := NewNode(ctx, NewNodeParams{
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
			Logger:              logger,
			DB:                  common.NewPersistenceMock(),
			RpcRequestTimeout:   150 * time.Millisecond,
		},
		RPCProxy: rpc_proxy.NewRPCImplParams{
			HostID:               1,
			HostURL:              "localhost:1234",
			Logger:               logger,
			RpcRequestTimeout:    150 * time.Millisecond,
			RpcDialTimeout:       time.Second,
			RpcReconnectDuration: 30 * time.Second,
		},
		HTTPProxy: http_proxy.NewHttpProxyParams{
			URL:    "localhost:8080",
			Logger: logger,
		},
		StateMachine: state_machine.NewKeyValueStateMachineParams{
			DB: common.NewPersistenceMock(),
		},
		Logger:     logger,
		DataFolder: "data/",
	})

	n.Start(ctx, false, false)

	_, err := rpc.Dial("tcp", ":1234")
	assert.NoError(t, err, "connect to rpc server ok")

	n.Stop(ctx)

	time.Sleep(3 * time.Second)

	_, err = rpc.Dial("tcp", ":1234")
	assert.Error(t, err, "connect to rpc server not ok")
}
