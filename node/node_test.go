package node

import (
	"khanh/raft-go/common"
	"khanh/raft-go/http_proxy"
	"khanh/raft-go/logic"
	"khanh/raft-go/persistance"
	"khanh/raft-go/rpc_proxy"
	"net/rpc"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestStop(t *testing.T) {
	n := NewNode(NewNodeParams{
		Brain: logic.NewRaftBrainParams{
			Info: common.ClusterMember{
				RpcUrl:  ":1234",
				HttpUrl: "localhost:8080",
				ID:      1,
			},
			DataFileName:        "logs.dat",
			HeartBeatTimeOutMin: 150,
			HeartBeatTimeOutMax: 300,
			ElectionTimeOutMin:  300,
			ElectionTimeOutMax:  500,
			Log:                 &zerolog.Logger{},
			DB:                  persistance.NewPersistenceMock(),
			StateMachine:        common.NewKeyValueStateMachine(),
		},
		RPCProxy: rpc_proxy.NewRPCImplParams{
			HostID:  1,
			HostURL: ":1234",
			Log:     &zerolog.Logger{},
		},
		HTTPProxy: http_proxy.NewHttpProxyParams{
			URL: "localhost:8080",
		},
	})
	n.Start(false)

	_, err := rpc.Dial("tcp", ":1234")
	assert.NoError(t, err, "connect to rpc server ok")

	n.Stop()

	time.Sleep(3 * time.Second)

	_, err = rpc.Dial("tcp", ":1234")
	assert.Error(t, err, "connect to rpc server not ok")
}
