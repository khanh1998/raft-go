package logic

import (
	"khanh/raft-go/common"
	"khanh/raft-go/persistance"
	"khanh/raft-go/rpc_proxy"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func Test_nodeImpl_BroadCastRequestVote(t *testing.T) {

}

func TestRaftBrainImpl_BroadCastRequestVote(t *testing.T) {
	type fields struct {
		logger              *zerolog.Logger
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
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "",
			fields: fields{
				logger:              &zerolog.Logger{},
				DB:                  persistance.NewPersistenceMock(),
				Peers:               []common.ClusterMember{{ID: 2, RpcUrl: ""}},
				State:               common.StateCandidate,
				ID:                  1,
				StateMachine:        common.NewKeyValueStateMachine(),
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
			n.BroadCastRequestVote()
		})
	}
}
