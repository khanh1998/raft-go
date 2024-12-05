package testing

import (
	"context"
	"khanh/raft-go/common"
	"khanh/raft-go/extensions/etcd/go_client"
	"slices"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func AssertGet(t *testing.T, c *Cluster, req go_client.GetRequest, expectedValue string, expectedValues []string, expectErr bool) {
	ctx := context.Background()
	if !req.Wait {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
	}
	res, err := c.HttpAgent.Get(ctx, req)
	if !expectErr {
		assert.NoError(t, err)
		if !req.Prefix {
			assert.Equal(t, expectedValue, res.Node.Value)
		} else {
			sort.Strings(expectedValues)
			resValues := []string{}
			for _, n := range res.Nodes {
				resValues = append(resValues, n.Value)
			}
			sort.Strings(resValues)
			if !assert.True(t, slices.Equal(expectedValues, resValues)) {
				t.Logf("have %v, want %v", resValues, expectedValues)
			}
		}
	} else {
		assert.Error(t, err)
	}
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
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	res, err := c.HttpAgent.Get(ctx, go_client.GetRequest{Key: key})
	if err != nil {
		res, err = c.HttpAgent.Set(ctx, go_client.SetRequest{Key: key, Value: common.GetPointer("0"), PrevExist: common.GetPointer(false)})
		assert.NoError(t, err)
		// value, err = c.HttpAgent.Get(ctx, go_client.GetRequest{Key: key})
	}
	assert.NoError(t, err)
	prevValue := res.Node.Value
	intVal, err := strconv.Atoi(prevValue)
	assert.NoError(t, err)
	nextVal := strconv.Itoa(intVal + 1)
	_, err = c.HttpAgent.Set(ctx, go_client.SetRequest{Key: key, Value: &nextVal, PrevExist: common.GetPointer(true), PrevValue: &prevValue})
	assert.NoError(t, err)
}

func AssertSet(t *testing.T, c *Cluster, req go_client.SetRequest) (createdIndex int, modifiedIndex int) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	res, err := c.HttpAgent.Set(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, res.Action, "set")
	assert.Equal(t, res.Node.Key, req.Key)
	assert.Greater(t, res.Node.CreatedIndex, 0)
	assert.GreaterOrEqual(t, res.Node.ModifiedIndex, res.Node.CreatedIndex)
	return res.Node.CreatedIndex, res.Node.ModifiedIndex
}

func AssertDelete(t *testing.T, c *Cluster, req go_client.DeleteRequest, expectError bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	res, err := c.HttpAgent.Delete(ctx, req)
	if expectError {
		assert.Error(t, err)
	} else {
		assert.NoError(t, err)
		assert.Equal(t, res.Action, "delete")
		assert.Equal(t, res.Node.Key, req.Key)
		assert.GreaterOrEqual(t, res.Node.CreatedIndex, 0)
		assert.GreaterOrEqual(t, res.Node.ModifiedIndex, 0)
	}
}

func AssertSetAndGet(t *testing.T, c *Cluster, key, value string) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	setValue := value

	res, err := c.HttpAgent.Set(ctx, go_client.SetRequest{Key: key, Value: &value})
	assert.NoError(t, err)

	getValue, err := c.HttpAgent.Get(ctx, go_client.GetRequest{Key: key, Wait: true, WaitIndex: res.Node.ModifiedIndex})
	assert.NoError(t, err)
	assert.Equal(t, setValue, getValue.Node.Value)
}

func AssertDelAndGet(t *testing.T, c *Cluster, key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := c.HttpAgent.Delete(ctx, go_client.DeleteRequest{Key: key})
	assert.NoError(t, err)

	_, err = c.HttpAgent.Get(ctx, go_client.GetRequest{Key: key})
	assert.Error(t, err)
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

}

// make sure the server is live and can response to requests
func AssertPing(t *testing.T, c *Cluster, id int) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	res, err := c.HttpAgent.GetInfo(ctx, id)
	assert.NoError(t, err)
	assert.Equal(t, id, res.ID)
}

// the leader catch up for the new node,
// after cautch up with the leader, new node will become a follower in cluster.
func AssertAddingNodeToCluster(t *testing.T, c *Cluster, id int) {

}

func AssertRemovingNodeFromCluster(t *testing.T, c *Cluster, id int) {

}

func AssertHavingNoLeader(t *testing.T, c *Cluster) {
	time.Sleep(c.MaxHeartbeatTimeout)
	_, err := c.HasOneLeader()
	assert.Error(t, err, "expect no leader in cluster")
}

func AssertLeaderChanged(t *testing.T, c *Cluster, prevLeaderId int, prevTerm int) (status common.GetStatusResponse) {
	var err error

	for i := 0; i < 10; i++ {
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
