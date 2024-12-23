package integration_testing

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// install snapshot to a follower that got some data before in static cluster
func TestLogCompactionForStaticCluster1(t *testing.T) {
	os.RemoveAll("data/")
	c := NewCluster("config/3-nodes-snap.yml")
	//defer c.Clean()

	// init 3 nodes cluster and push some data
	AssertHavingOneLeader(t, c)
	IncreaseBy(t, c, "count", 10)
	fId, err := c.StopFollower()
	assert.NoError(t, err)

	IncreaseBy(t, c, "count", 15)

	//time.Sleep(c.MaxElectionTimeout) // wait leader to take snapshot

	err = c.StartNode(fId)
	assert.NoError(t, err)

	time.Sleep(c.MaxElectionTimeout * 3) // wait for the follower syncing data

	_, err = c.StopLeader()
	assert.NoError(t, err)

	AssertHavingOneLeader(t, c)
	AssertGet(t, c, "count", "25")
}

// install snapshot to follower that has no data in static cluster
func TestLogCompactionForStaticCluster2(t *testing.T) {
	os.RemoveAll("data/")
	c := NewCluster("config/3-nodes-snap.yml")
	defer c.Clean()

	// init 3 nodes cluster,
	// kill a follower immediately,
	// push enough data to cluster, so there will be a snapshot.
	AssertHavingOneLeader(t, c)
	fId, err := c.StopFollower()
	IncreaseBy(t, c, "count", 10)
	assert.NoError(t, err)

	// restart the killed follower,
	// this follower has no data (no log, no snapshot)
	err = c.StartNode(fId)
	assert.NoError(t, err)

	time.Sleep(c.MaxElectionTimeout) // wait for snapshot installing to follower

	IncreaseBy(t, c, "count", 5)

	_, err = c.StopLeader()
	assert.NoError(t, err)

	AssertHavingOneLeader(t, c)
	AssertGet(t, c, "count", "15")
}
func TestLogCompactionForStaticCluster3(t *testing.T) {
	os.RemoveAll("data/")
	c := NewCluster("config/3-nodes-snap.yml")
	//defer c.Clean()

	// init 3 nodes cluster and push some data
	AssertHavingOneLeader(t, c)
	IncreaseBy(t, c, "count", 10)
	_, err := c.StopFollower()
	assert.NoError(t, err)

	IncreaseBy(t, c, "count", 15)

	// purpose of these stop and start are to make nodes restore data back from file,
	// make sure data serialize and deserialize work well.
	c.StopAll()
	c.StartAll()

	time.Sleep(3 * c.MaxElectionTimeout) // wait a little more for log conflict resolving
	AssertHavingOneLeader(t, c)
	AssertGet(t, c, "count", "25")
}

func TestLogCompactionForDynamicCluster(t *testing.T) {
	os.RemoveAll("data/")
	c := NewDynamicCluster("config/dynamic-snap.yml")
	defer c.Clean()
	AssertHavingOneLeader(t, c) // this will also wait until a follower win election

	IncreaseBy(t, c, "count", 10)
	AssertLiveNode(t, c, 1)
	// adding second node to cluster
	AssertCreatingNode(t, c, 2)
	AssertAddingNodeToCluster(t, c, 2)
	AssertGet(t, c, "count", "10")
	IncreaseBy(t, c, "count", 10)
	AssertLiveNode(t, c, 2)
	// adding third node to cluster
	AssertCreatingNode(t, c, 3)
	AssertAddingNodeToCluster(t, c, 3)
	AssertGet(t, c, "count", "20")
	IncreaseBy(t, c, "count", 10)
	AssertLiveNode(t, c, 3)
	// adding fourth node to cluster
	AssertCreatingNode(t, c, 4)
	AssertAddingNodeToCluster(t, c, 4)
	AssertGet(t, c, "count", "30")
	IncreaseBy(t, c, "count", 10)
	AssertLiveNode(t, c, 4)
	// adding fifth node to cluster
	AssertCreatingNode(t, c, 5)
	AssertAddingNodeToCluster(t, c, 5)
	AssertGet(t, c, "count", "40")
	IncreaseBy(t, c, "count", 10)
	AssertLiveNode(t, c, 5)

	AssertGet(t, c, "count", "50")
}

func TestClusterRestart(t *testing.T) {
	os.RemoveAll("data/")
	c := NewCluster("config/3-nodes-1.yml")
	// defer c.Clean()

	AssertHavingOneLeader(t, c)
	IncreaseBy(t, c, "count", 10)
	time.Sleep(500 * time.Millisecond) // wait for wal and snapshot cleanup in background
	c.StopAll()

	c.StartAll()
	AssertHavingOneLeader(t, c)
	IncreaseBy(t, c, "count", 10)
	time.Sleep(500 * time.Millisecond) // wait for wal and snapshot cleanup in background
	c.StopAll()

	c.StartAll()
	AssertHavingOneLeader(t, c)
	IncreaseBy(t, c, "count", 10)
	time.Sleep(500 * time.Millisecond) // wait for wal and snapshot cleanup in background
	c.StopAll()

	c.StartAll()
	AssertHavingOneLeader(t, c)
	AssertGet(t, c, "count", "30")
	c.StopAll()
}
