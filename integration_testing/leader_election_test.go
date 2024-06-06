package integration_testing

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLeaderElection5(t *testing.T) {
	c := NewCluster("config/5-nodes.yml")
	defer c.Clean()

	AssertHavingOneLeader(t, c)
}

func TestLeaderElection3(t *testing.T) {
	c := NewCluster("config/3-nodes.yml")
	defer c.Clean()

	AssertHavingOneLeader(t, c)
}

func TestReLeaderElection(t *testing.T) {
	c := NewCluster("config/3-nodes.yml")
	defer c.Clean()

	status1 := AssertHavingOneLeader(t, c)

	// stop the leader, and check status immediately
	err := c.StopNode(status1.ID)
	assert.NoError(t, err)
	AssertHavingNoLeader(t, c)

	// re-elect new leader
	AssertHavingOneLeader(t, c)
}

func TestStopAndStartNode(t *testing.T) {
	c := NewCluster("config/3-nodes.yml")
	defer c.Clean()
	AssertHavingOneLeader(t, c)
	AssertLiveNode(t, c, 3)

	stopNodeId, err := c.StopLeader()
	assert.NoError(t, err)
	AssertLiveNode(t, c, 2)
	AssertHavingOneLeader(t, c)

	c.StartNode(stopNodeId)
	AssertLiveNode(t, c, 3)
}

// given a three nodes cluster, 2 of 3 nodes are crashed,
// the cluster now is unable to commit new data.
func TestMajorityOfClusterIsCrashed(t *testing.T) {
	c := NewCluster("config/3-nodes.yml")
	defer c.Clean()
	AssertHavingOneLeader(t, c)
	AssertLiveNode(t, c, 3)

	// one leader, two followers
	IncreaseBy(t, c, "count", 10)

	_, err := c.StopLeader()
	assert.NoError(t, err)
	AssertLiveNode(t, c, 2)

	// one leader, one follower
	AssertHavingOneLeader(t, c)
	IncreaseBy(t, c, "count", 10)
	AssertGet(t, c, "count", "20")

	_, err = c.StopFollower()
	assert.NoError(t, err)
	AssertLiveNode(t, c, 1)
	// we only stop the follower,
	// but after a election timeout elapsed,
	// the leader need to step down as it can't make a successful round of heartbeat to majority
	time.Sleep(c.MaxElectionTimeout)
	AssertHavingNoLeader(t, c)
}
