package integration_testing

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddServerAndCommitData(t *testing.T) {
	c := NewDynamicCluster("config/dynamic.yml")
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

func TestAddServerAndRemoveServer(t *testing.T) {
	// create a three nodes dynamic cluster
	// os.RemoveAll("data/")
	c := NewDynamicCluster("config/dynamic.yml")
	defer c.Clean()
	AssertHavingOneLeader(t, c) // this will also wait until a follower win election

	IncreaseBy(t, c, "count", 3)

	AssertCreatingNode(t, c, 2)
	AssertAddingNodeToCluster(t, c, 2)
	IncreaseBy(t, c, "count", 3)

	AssertCreatingNode(t, c, 3)
	AssertAddingNodeToCluster(t, c, 3)
	IncreaseBy(t, c, "count", 3)

	// check the validity of the cluster
	AssertLiveNode(t, c, 3)
	AssertGet(t, c, "count", "9")

	l1 := AssertHavingOneLeader(t, c)
	fmt.Println("l1=", l1)
	err := c.RemoveServer(l1.ID)
	assert.NoError(t, err)
	l2 := AssertLeaderChanged(t, c, l1.ID, l1.Term)
	AssertGet(t, c, "count", "9")
	IncreaseBy(t, c, "count", 3)

	// remove node
	err = c.RemoveServer(l2.ID)
	assert.NoError(t, err)
	AssertLeaderChanged(t, c, l2.ID, l2.Term)
	AssertGet(t, c, "count", "12")
	IncreaseBy(t, c, "count", 3)

	// there is one node left in the cluster
	AssertGet(t, c, "count", "15")
}

// todo: test uncommit configuration get roll back
