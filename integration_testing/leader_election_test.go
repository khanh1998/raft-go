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

func AssertHavingOneLeader(t *testing.T, c *Cluster) common.GetStatusResponse {
	time.Sleep(3 * c.MaxElectionTimeout)
	status, err := c.HasOneLeader()

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
	c.StopNode(status1.ID)
	AssertHavingNoLeader(t, c)

	// re-elect new leader
	AssertHavingOneLeader(t, c)
}
