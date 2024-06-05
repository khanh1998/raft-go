package integration_testing

import (
	"testing"
)

func TestAddServerAndCommitData(t *testing.T) {
	c := NewDynamicCluster("config/dynamic.yml")
	defer c.Clean()

	AssertHavingOneLeader(t, c)
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
	AssertGet(t, c, "count", "30")
	AssertLiveNode(t, c, 3)
}

func TestAddServerAndRemoveServer(t *testing.T) {
	c := NewDynamicCluster("config/dynamic.yml")
	defer c.Clean()

	AssertHavingOneLeader(t, c)
	AssertPing(t, c, 1)
	// set some data
	IncreaseBy(t, c, "count", 10)
	// adding second node to cluster
	AssertCreatingNode(t, c, 2)
	AssertAddingNodeToCluster(t, c, 2)
	IncreaseBy(t, c, "count", 10)
	// adding second node to cluster
	AssertCreatingNode(t, c, 3)
	AssertAddingNodeToCluster(t, c, 3)
	IncreaseBy(t, c, "count", 10)
	// finished creating cluster
	AssertGet(t, c, "count", "30")
	AssertLiveNode(t, c, 3)
	// remove node
}

func TestRemoveNode(t *testing.T) {

}
