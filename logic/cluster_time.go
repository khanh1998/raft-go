package logic

import (
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
