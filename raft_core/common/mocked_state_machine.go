package common

import (
	"context"
	gc "khanh/raft-go/common"
)

type MockedStateMachine struct{}

func (m MockedStateMachine) Reset(ctx context.Context) error {
	return nil
}
func (m MockedStateMachine) Process(ctx context.Context, logIndex int, log gc.Log) (result gc.LogResult, err error) {
	return
}
func (m MockedStateMachine) StartSnapshot(ctx context.Context) error {
	return nil
}
func (m MockedStateMachine) GetLastConfig() map[int]gc.ClusterMember {
	return nil
}
