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
		logger            *zerolog.Logger
		DB                Persistence
		Peers             []common.PeerInfo
		State             RaftState
		ID                int
		StateMachine      SimpleStateMachine
		ElectionTimeOut   *time.Timer
		HeartBeatTimeOut  *time.Timer
		Quorum            int
		MinRandomDuration int64
		MaxRandomDuration int64
		RpcProxy          RPCProxy
		CurrentTerm       int
		VotedFor          int
		Logs              []common.Log
		CommitIndex       int
		LastApplied       int
		NextIndex         map[int]int
		MatchIndex        map[int]int
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "",
			fields: fields{
				logger:            &zerolog.Logger{},
				DB:                persistance.NewPersistenceMock(),
				Peers:             []common.PeerInfo{{ID: 2, URL: ""}},
				State:             StateCandidate,
				ID:                1,
				StateMachine:      common.NewKeyValueStateMachine(),
				ElectionTimeOut:   nil,
				HeartBeatTimeOut:  nil,
				Quorum:            3,
				MinRandomDuration: 8,
				MaxRandomDuration: 8,
				RpcProxy:          rpc_proxy.RPCProxyMock{},
				CurrentTerm:       7,
				VotedFor:          0,
				Logs:              []common.Log{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &RaftBrainImpl{
				logger:            tt.fields.logger,
				DB:                tt.fields.DB,
				Peers:             tt.fields.Peers,
				State:             tt.fields.State,
				ID:                tt.fields.ID,
				StateMachine:      tt.fields.StateMachine,
				ElectionTimeOut:   tt.fields.ElectionTimeOut,
				HeartBeatTimeOut:  tt.fields.HeartBeatTimeOut,
				Quorum:            tt.fields.Quorum,
				MinRandomDuration: tt.fields.MinRandomDuration,
				MaxRandomDuration: tt.fields.MaxRandomDuration,
				RpcProxy:          tt.fields.RpcProxy,
				CurrentTerm:       tt.fields.CurrentTerm,
				VotedFor:          tt.fields.VotedFor,
				Logs:              tt.fields.Logs,
				CommitIndex:       tt.fields.CommitIndex,
				LastApplied:       tt.fields.LastApplied,
				NextIndex:         tt.fields.NextIndex,
				MatchIndex:        tt.fields.MatchIndex,
			}
			n.BroadCastRequestVote()
		})
	}
}
