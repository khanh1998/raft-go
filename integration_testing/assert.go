package integration_testing

import (
	"context"
	"khanh/raft-go/common"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func AssertGet(t *testing.T, c *Cluster, key, expectedValue string) {
	value, err := c.HttpAgent.ClientQuery(key)
	assert.NoError(t, err)
	assert.Equal(t, expectedValue, value)
}

func AssertSetAndGetRange(t *testing.T, c *Cluster, key string, from int, to int) {
	for i := from; i < to; i++ {
		value := strconv.Itoa(i)
		AssertSetAndGet(t, c, key, value)
	}
}

func IncreaseBy(t *testing.T, c *Cluster, key string, count int) {
	for i := 0; i < count; i++ {
		IncreaseByOne(t, c, key)
	}
}

func IncreaseByOne(t *testing.T, c *Cluster, key string) {
	value, err := c.HttpAgent.ClientQuery(key)
	if err != nil {
		err = c.HttpAgent.ClientRequest("set", key, "0")
		assert.NoError(t, err)
		value, err = c.HttpAgent.ClientQuery(key)
	}
	assert.NoError(t, err)
	intVal, err := strconv.Atoi(value)
	assert.NoError(t, err)
	nextVal := strconv.Itoa(intVal + 1)
	err = c.HttpAgent.ClientRequest("set", key, nextVal)
	assert.NoError(t, err)
}

func AssertSetAndGet(t *testing.T, c *Cluster, key, value string) {
	setValue := value

	seqNum := c.HttpAgent.sequenceNum
	err := c.HttpAgent.ClientRequest("set", key, setValue)
	assert.NoError(t, err)

	getValue, err := c.HttpAgent.ClientQuery(key)
	assert.NoError(t, err)
	assert.Equal(t, setValue, getValue)
	assert.Greater(t, c.HttpAgent.sequenceNum, seqNum)
}

func AssertDelAndGet(t *testing.T, c *Cluster, key string) {
	seqNum := c.HttpAgent.sequenceNum
	err := c.HttpAgent.ClientRequest("del", key, "")
	assert.NoError(t, err)

	_, err = c.HttpAgent.ClientQuery(key)
	assert.Error(t, err)
	assert.Greater(t, c.HttpAgent.sequenceNum, seqNum)
}

func AssertClientRegister(t *testing.T, c *Cluster) {
	assert.Equal(t, c.HttpAgent.clientId, 0)
	err := c.HttpAgent.RegisterClient()
	assert.NoError(t, err)
	assert.Greater(t, c.HttpAgent.clientId, 0)
}

// count how many servers in the cluster can response to an ping request
func AssertLiveNode(t *testing.T, c *Cluster, expectCount int) {
	count, err := c.CountLiveNode()
	assert.NoError(t, err)
	assert.Equal(t, expectCount, count)
}

// create a new node, and it do nothing and is waiting for leader to send logs to it (catch-up process),
// new node isn't part of the cluster yet, it won't request vote or response to request vote.
func AssertCreatingNode(t *testing.T, c *Cluster, id int) {
	err := c.createNewNode(context.Background(), id)
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

func AssertRemovingNodeFromCluster(t *testing.T, c *Cluster, id int) {
	err := c.RemoveServer(id)
	assert.NoError(t, err)
}

func AssertHavingNoLeader(t *testing.T, c *Cluster) {
	time.Sleep(c.MaxHeartbeatTimeout)
	_, err := c.HasOneLeader()
	assert.Error(t, err, "expect no leader in cluster")
}

func AssertLeaderChanged(t *testing.T, c *Cluster, prevLeaderId int, prevTerm int) (status common.GetStatusResponse) {
	var err error

	for i := 0; i < 5; i++ {
		time.Sleep(c.MaxElectionTimeout)
		status, err = c.HasOneLeader()
		if err == nil || err != ErrThereIsNoLeader {
			break
		}
	}

	assert.NoError(t, err, "expect one leader int the cluster")
	assert.Greater(t, status.Term, prevTerm)
	assert.NotEqual(t, status.ID, prevLeaderId)

	return status
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
