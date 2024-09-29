package logic

import (
	"context"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"khanh/raft-go/rpc_proxy"
	"khanh/raft-go/state_machine"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_nodeImpl_BroadCastRequestVote(t *testing.T) {

}

func TestRaftBrainImpl_BroadCastRequestVote(t *testing.T) {
	type fields struct {
		logger              observability.Logger
		DB                  Persistence
		Peers               []common.ClusterMember
		State               common.RaftState
		ID                  int
		StateMachine        SimpleStateMachine
		ElectionTimeOut     *time.Timer
		HeartBeatTimeOut    *time.Timer
		Quorum              int
		HeartBeatTimeOutMin int64
		HeartBeatTimeOutMax int64
		ElectionTimeOutMin  int64
		ElectionTimeOutMax  int64
		RpcProxy            RPCProxy
		CurrentTerm         int
		VotedFor            int
		Logs                []common.Log
		CommitIndex         int
		LastApplied         int
		NextIndex           map[int]int
		MatchIndex          map[int]int
	}

	sm, err := state_machine.NewKeyValueStateMachine(state_machine.NewKeyValueStateMachineParams{DB: common.NewPersistenceMock()})
	assert.NoError(t, err)

	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "",
			fields: fields{
				logger:              observability.NewZerolog("", 1),
				DB:                  common.NewPersistenceMock(),
				Peers:               []common.ClusterMember{{ID: 2, RpcUrl: ""}},
				State:               common.StateCandidate,
				ID:                  1,
				StateMachine:        sm,
				ElectionTimeOut:     nil,
				HeartBeatTimeOut:    nil,
				Quorum:              3,
				HeartBeatTimeOutMin: 8,
				HeartBeatTimeOutMax: 9,
				ElectionTimeOutMin:  40,
				ElectionTimeOutMax:  41,
				RpcProxy:            rpc_proxy.RPCProxyMock{},
				CurrentTerm:         7,
				VotedFor:            0,
				Logs:                []common.Log{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &RaftBrainImpl{
				logger:              tt.fields.logger,
				db:                  tt.fields.DB,
				members:             tt.fields.Peers,
				state:               tt.fields.State,
				id:                  tt.fields.ID,
				stateMachine:        tt.fields.StateMachine,
				electionTimeOut:     tt.fields.ElectionTimeOut,
				heartBeatTimeOut:    tt.fields.HeartBeatTimeOut,
				heartBeatTimeOutMin: tt.fields.HeartBeatTimeOutMin,
				heartBeatTimeOutMax: tt.fields.HeartBeatTimeOutMax,
				electionTimeOutMin:  tt.fields.ElectionTimeOutMin,
				electionTimeOutMax:  tt.fields.ElectionTimeOutMax,
				rpcProxy:            tt.fields.RpcProxy,
				currentTerm:         tt.fields.CurrentTerm,
				votedFor:            tt.fields.VotedFor,
				logs:                tt.fields.Logs,
				commitIndex:         tt.fields.CommitIndex,
				lastApplied:         tt.fields.LastApplied,
				nextIndex:           tt.fields.NextIndex,
				matchIndex:          tt.fields.MatchIndex,
			}
			n.BroadCastRequestVote(context.TODO())
		})
	}
}
