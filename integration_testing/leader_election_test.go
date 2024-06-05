package integration_testing

import (
	"khanh/raft-go/common"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func AssertHavingNoLeader(t *testing.T, c *Cluster) {
	time.Sleep(c.MaxHeartbeatTimeout)
	_, err := c.HasOneLeader()
	assert.ErrorIs(t, err, ErrThereIsNoLeader, "expect no leader in cluster")
}

func AssertHavingOneLeader(t *testing.T, c *Cluster) (status common.GetStatusResponse) {
	var err error

	for i := 0; i < 5; i++ {
		time.Sleep(c.MaxElectionTimeout)
		status, err = c.HasOneLeader()
		if err == nil || err != ErrThereIsNoLeader {
			break
		}
	}

	assert.NoError(t, err, "expect one leader int the cluster")
	assert.Greater(t, status.Term, 0)
	assert.Greater(t, status.ID, 0)

	return status
}

func TestLeaderElection5(t *testing.T) {
	c := NewCluster("5-nodes.yml")
	defer c.Clean()

	AssertHavingOneLeader(t, c)
}

func TestLeaderElection3(t *testing.T) {
	c := NewCluster("3-nodes.yml")
	defer c.Clean()

	AssertHavingOneLeader(t, c)
}

func TestReLeaderElection(t *testing.T) {
	c := NewCluster("3-nodes.yml")
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
	c := NewCluster("3-nodes.yml")
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
