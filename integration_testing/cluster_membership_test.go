package integration_testing

import (
	"khanh/raft-go/common"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// create a new node, and it do nothing and is waiting for leader to send logs to it (catch-up process),
// new node isn't part of the cluster yet, it won't request vote or response to request vote.
func AssertCreatingNode(t *testing.T, c *Cluster, id int) {
	err := c.createNewNode(id)
	assert.NoError(t, err)

	timeout := 150 * time.Millisecond
	res, err := c.RpcAgent.SendPing(id, &timeout)
	assert.NoError(t, err)
	assert.Equal(t, id, res.ID)
	assert.Equal(t, common.StateCatchingUp, res.State)
	assert.Equal(t, 0, res.Term)
	assert.Equal(t, 0, res.LeaderId)
}

// make sure the server is live and can response to requests
func AssertPing(t *testing.T, c *Cluster, id int) {
	timeout := 150 * time.Millisecond
	res, err := c.RpcAgent.SendPing(id, &timeout)
	assert.NoError(t, err)
	assert.Equal(t, id, res.ID)
}

// the leader catch up for the new node,
// after cautch up with the leader, new node will become a follower in cluster.
func AssertAddingNodeToCluster(t *testing.T, c *Cluster, id int) {
	err := c.AddServer(id)
	assert.NoError(t, err)

	timeout := 150 * time.Millisecond
	res, err := c.RpcAgent.SendPing(id, &timeout)
	assert.NoError(t, err)
	assert.Equal(t, id, res.ID)
	assert.Equal(t, common.StateFollower, res.State)
	assert.Greater(t, res.Term, 0)
	assert.Greater(t, res.LeaderId, 0)
}

func TestCreatingDynamicCluster(t *testing.T) {
	c := NewDynamicCluster("dynamic.yml")
	defer c.Clean()

	AssertPing(t, c, 1)
	time.Sleep(c.MaxElectionTimeout)
	// adding second node to cluster
	AssertCreatingNode(t, c, 2)
	AssertAddingNodeToCluster(t, c, 2)
	// adding second node to cluster
	AssertCreatingNode(t, c, 3)
	AssertAddingNodeToCluster(t, c, 3)
}

func TestCreatingDynamicClusterAndCommitData(t *testing.T) {
	c := NewDynamicCluster("dynamic.yml")
	defer c.Clean()

	AssertPing(t, c, 1)
	time.Sleep(c.MaxElectionTimeout)
	// set some data
	AssertSetAndGetRange(t, c, "count", 1, 10)
	// adding second node to cluster
	AssertCreatingNode(t, c, 2)
	AssertAddingNodeToCluster(t, c, 2)
	// adding second node to cluster
	AssertCreatingNode(t, c, 3)
	AssertAddingNodeToCluster(t, c, 3)
}

func TestRemoveNode(t *testing.T) {

}
