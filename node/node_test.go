package node

import (
	"context"
	"khanh/raft-go/common"
	classicCommon "khanh/raft-go/extensions/classic/common"
	classicHttp "khanh/raft-go/extensions/classic/http_server"
	classicSt "khanh/raft-go/extensions/classic/state_machine"
	"khanh/raft-go/observability"
	"khanh/raft-go/raft_core/logic"
	"khanh/raft-go/raft_core/persistence_state"
	"khanh/raft-go/raft_core/rpc_proxy"
	"khanh/raft-go/raft_core/storage"
	"net/rpc"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRpcConnection(t *testing.T) {
	logger := observability.NewZerolog(common.ObservabilityConfig{}, 1)
	ctx := context.Background()
	logFactory := classicCommon.ClassicLogFactory{}
	persistState := persistence_state.NewRaftPersistenceState(persistence_state.NewRaftPersistenceStateParams{
		CurrentTerm: 2,
		Logs:        []common.Log{},
		Storage: storage.NewStorageForTest(
			storage.NewStorageParams{WalSize: 1024, DataFolder: "data/", Logger: logger},
			storage.NewFileWrapperMock(),
		),
		LogFactory: logFactory,
	})

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
			RpcRequestTimeout:   150 * time.Millisecond,
			PersistenceState:    persistState,
			LogLengthLimit:      1000,
			LogFactory:          logFactory,
		},
		RPCProxy: rpc_proxy.NewRPCImplParams{
			HostID:               1,
			HostURL:              "localhost:1234",
			Logger:               logger,
			RpcRequestTimeout:    150 * time.Millisecond,
			RpcDialTimeout:       time.Second,
			RpcReconnectDuration: 30 * time.Second,
		},
		ClassicSetup: &ClassicSetup{
			HttpServer: classicHttp.NewClassicHttpProxyParams{
				URL:    "localhost:8080",
				Logger: logger,
			},
			StateMachine: classicSt.NewClassicStateMachineParams{
				PersistState: persistState,
				Logger:       logger,
			},
		},
		LogExtensionEnabled: common.LogExtensionClassic,
		Logger:              logger,
	})

	n.Start(ctx, false, false)

	_, err := rpc.Dial("tcp", ":1234")
	assert.NoError(t, err, "connect to rpc server ok")

	n.Stop(ctx)

	time.Sleep(3 * time.Second)

	_, err = rpc.Dial("tcp", ":1234")
	assert.Error(t, err, "connect to rpc server not ok")
}
