package common

import (
	"context"
	"errors"
	gc "khanh/raft-go/common"
	"khanh/raft-go/observability"
	"math/rand/v2"
	"time"
)

type SimpleStateMachine interface {
	Reset(ctx context.Context) error
	Process(ctx context.Context, logIndex int, log gc.Log) (result gc.LogResult, err error)
	StartSnapshot(ctx context.Context) error
	GetLastConfig() map[int]gc.ClusterMember
}

type RaftBrain interface {
	// for client
	ClientRequest(ctx context.Context, input gc.Log, output *gc.ClientRequestOutput) (err error)
	RegisterClient(ctx context.Context, input gc.Log, output *gc.RegisterClientOutput) (err error)
	KeepAlive(ctx context.Context, input gc.Log, output *gc.KeepAliveClientOutput) (err error)
	ClientQuery(ctx context.Context, input gc.Log, output *gc.ClientQueryOutput) (err error)
	AddServer(ctx context.Context, input gc.Log, output *gc.AddServerOutput) (err error)
	RemoveServer(ctx context.Context, input gc.Log, output *gc.RemoveServerOutput) (err error)
	GetInfo() gc.GetStatusResponse
	GetMembers() []gc.ClusterMember

	// for node
	Start(ctx context.Context)
	Stop()
	SetStateMachine(sm SimpleStateMachine)
}

// for testing purpose: to simulate network partition and unreliable,
type NetworkSimulation struct {
	// the current node are allow to send and receive request from/to the nodes in allows list.
	Allows map[int]struct{}

	// the delay we add to every RPC request (50ms - 100ms)
	MinDelay time.Duration
	MaxDelay time.Duration

	// we sometime drop some message to simulate the unreliable of network
	MsgDropRate uint // 0% -> 100%

	//TODO:
	// duplicate message
	// reordering message
	// Asymmetric Partitions: can send but can't receive or vice versa
	// slow node: slow message processing
	Logger observability.Logger
}

// NormalRandomWithBoundsInt64 generates a random int64 value following a normal distribution,
// with a given mean, stddev, and constraints of min and max. It does not use a loop.
func NormalRandomWithBoundsInt64(mean, stddev, min, max int64) int64 {
	// Generate a value from the normal distribution using rand.NormFloat64
	value := mean + int64(rand.NormFloat64()*float64(stddev))

	// If the value is within the bounds, return it
	if value >= min && value <= max {
		return value
	}

	// Otherwise, return the closest boundary (either min or max)
	if value < min {
		return min
	}
	return max
}

func (n NetworkSimulation) ProcessInbound(id int) error {
	n.Logger.Info("ProcessInbound begin", "id", id)
	if _, ok := n.Allows[id]; !ok {
		return errors.New("network: restricted")
	}

	randomNum := gc.RandInt(0, 100)
	if int(randomNum) < int(n.MsgDropRate) {
		n.Logger.Info("Message dropped", "rand", randomNum, "rate", n.MsgDropRate)

		return nil
	}

	min, max := n.MinDelay.Nanoseconds(), n.MaxDelay.Nanoseconds()
	stddev := (max - min) / 4
	mean := min + ((max - min) / 2)
	delay := NormalRandomWithBoundsInt64(mean, stddev, min, max)
	n.Logger.Info("Message delayed", "duration", time.Duration(delay).String())
	time.Sleep(time.Duration(delay))
	n.Logger.Info("ProcessInbound done", "id", id)

	return nil
}

func (n NetworkSimulation) ProcessOutbound(id int) error {
	// if _, ok := n.Restricts[id]; ok {
	// 	return errors.New("network: restricted")
	// }

	return nil
}

type InternalRpcServer interface {
	// for node
	Start(ctx context.Context)
	Stop()
	SetAccessible()
	SetInaccessible()
	SetNetworkSimulation(network NetworkSimulation)
	GetNetworkSimulation() *NetworkSimulation
	UnsetNetworkSimulation()
}
