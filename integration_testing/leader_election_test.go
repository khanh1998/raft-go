package integration_testing

import (
	"testing"

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
