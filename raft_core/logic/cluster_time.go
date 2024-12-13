package logic

import (
	"context"
	"fmt"
	"khanh/raft-go/common"
	"sync"
	"time"
)

type ClusterClock struct {
	clusterTimeAtEpoch uint64 // be careful, this variable will overflow in 584 years :D
	localTimeAtEpoch   time.Time
	mu                 sync.Mutex
}

func NewClusterClock() *ClusterClock {
	return &ClusterClock{
		clusterTimeAtEpoch: 0,
		localTimeAtEpoch:   time.Now(),
	}
}

func (c *ClusterClock) NewEpoch(clusterTime uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if clusterTime < c.clusterTimeAtEpoch {
		panic(
			fmt.Sprintf(
				"clusterTime < c.clusterTimeAtEpoch, %v, %v",
				clusterTime, c.clusterTimeAtEpoch),
		)
	}

	c.clusterTimeAtEpoch = clusterTime
	c.localTimeAtEpoch = time.Now()
}

func (c *ClusterClock) LeaderStamp() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Calculate how much time has passed since the last epoch
	nanosSinceEpoch := time.Since(c.localTimeAtEpoch).Nanoseconds()

	// Update cluster time based on the elapsed time
	c.clusterTimeAtEpoch += uint64(nanosSinceEpoch)
	c.localTimeAtEpoch = time.Now() // reset local time at epoch

	return c.clusterTimeAtEpoch
}

// Interpolate estimates the current cluster time without updating state.
func (c *ClusterClock) Interpolate() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Calculate elapsed time since the epoch
	nanosSinceEpoch := time.Since(c.localTimeAtEpoch).Nanoseconds()

	// Return the estimated cluster time based on elapsed local time
	return c.clusterTimeAtEpoch + uint64(nanosSinceEpoch)
}

func (c *ClusterClock) ElapsedSinceEpoch() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return uint64(time.Since(c.localTimeAtEpoch).Nanoseconds())
}

// every time we received a log from client, we will attach the cluster time into the log,
// when the log get committed, the cluster time also get recorded.
// that means if for a long time without any committed log, the cluster time also doesn't get recorded,
// that's why we need to sometime append a new log just to record the cluster time, otherwise,
// the cluster time can be lost if the leader crashed.
func (r *RaftBrainImpl) autoCommitClusterTime(ctx context.Context) {
	duration := 30 * time.Second // todo: make this configurable
	limit := uint64(duration.Nanoseconds())
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(duration):
			if r.state != common.StateLeader {
				continue
			}

			r.log().Info(
				"autoCommitClusterTime called",
				"elapsed", r.clusterClock.ElapsedSinceEpoch(),
				"limit", limit,
				"elapsed > limit", r.clusterClock.ElapsedSinceEpoch() > limit,
			)

			if r.clusterClock.ElapsedSinceEpoch() > limit {
				_, err := r.appendLog(ctx, r.logFactory.CreateTimeCommit(r.GetCurrentTerm(), r.clusterClock.LeaderStamp()))

				if err != nil {
					r.log().ErrorContext(ctx, "autoCommitClusterTime_appendLog", err)
				}
			}
		}
	}
}

// reset current cluster time to the time of the last log,
// use this after deleting logs, or becoming leader
func (r *RaftBrainImpl) resetClusterTime() {
	lastLog, err := r.persistState.GetLastLog()
	if err == nil {
		r.log().Info("resetClusterTime", "lastLog", lastLog)
		r.clusterClock.NewEpoch(lastLog.GetTime())
	} else {
		// if there is no log
		r.log().Error("resetClusterTime: will get time from snapshot", err)
		clusterTime := r.persistState.GetLatestSnapshotMetadata().LastLogTime
		r.clusterClock.NewEpoch(clusterTime)
	}
}
