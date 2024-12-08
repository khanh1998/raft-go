package testing

import (
	"khanh/raft-go/extensions/etcd/go_client"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddServer(t *testing.T) {
	os.RemoveAll("data/")
	c := NewDynamicCluster("config/dynamic.yml")

	// one node cluster
	AssertHavingOneLeader(t, c)
	AssertIncreaseCounter(t, c, "counter", 5)

	// two nodes cluster
	AssertAddingNodeToCluster(t, c, 2)
	AssertIncreaseCounter(t, c, "counter", 5)

	// three nodes cluster
	AssertAddingNodeToCluster(t, c, 3)
	AssertIncreaseCounter(t, c, "counter", 5)

	AssertLiveNode(t, c, 3)
	AssertGet(t, c, go_client.GetRequest{Key: "counter"}, "15", nil, false)
}

func TestRemoveLeader(t *testing.T) {
	os.RemoveAll("data/")
	c := NewDynamicCluster("config/dynamic.yml")

	// create a dynamic cluster with three nodes
	l1 := AssertHavingOneLeader(t, c)
	AssertAddingNodeToCluster(t, c, 2)
	AssertAddingNodeToCluster(t, c, 3)
	AssertIncreaseCounter(t, c, "counter", 5)

	AssertLiveNode(t, c, 3)
	AssertGet(t, c, go_client.GetRequest{Key: "counter"}, "5", nil, false)

	// two node cluster
	AssertRemovingNodeFromCluster(t, c, l1.ID) // remove leader
	l2 := AssertHavingOneLeader(t, c)
	AssertLiveNode(t, c, 2)
	AssertLeaderChanged(t, c, l1.ID, l1.Term)

	AssertIncreaseCounter(t, c, "counter", 1)
	AssertGet(t, c, go_client.GetRequest{Key: "counter"}, "6", nil, false)

	// one node cluster
	AssertRemovingNodeFromCluster(t, c, l2.ID) // remove leader
	l3 := AssertHavingOneLeader(t, c)
	AssertLeaderChanged(t, c, l2.ID, l2.Term)
	AssertIncreaseCounter(t, c, "counter", 1)
	_ = l3

	AssertGet(t, c, go_client.GetRequest{Key: "counter"}, "7", nil, false)
}

func TestRemoveFollower(t *testing.T) {
	os.RemoveAll("data/")
	c := NewDynamicCluster("config/dynamic.yml")

	l1 := AssertHavingOneLeader(t, c)
	AssertAddingNodeToCluster(t, c, 2)
	AssertAddingNodeToCluster(t, c, 3)
	AssertIncreaseCounter(t, c, "counter", 5)

	AssertLiveNode(t, c, 3)
	AssertGet(t, c, go_client.GetRequest{Key: "counter"}, "5", nil, false)

	AssertRemovingNodeFromCluster(t, c, 2) // remove follower 2
	// now we have a two nodes cluster
	AssertLiveNode(t, c, 2)
	l2 := AssertHavingOneLeader(t, c)
	assert.Equal(t, l1.ID, l2.ID)
	assert.Equal(t, l1.Term, l2.Term)

	AssertIncreaseCounter(t, c, "counter", 1)
	AssertGet(t, c, go_client.GetRequest{Key: "counter"}, "6", nil, false)

	AssertRemovingNodeFromCluster(t, c, 3) // remove follower 3
	// now we have one node cluster
	AssertIncreaseCounter(t, c, "counter", 1)
	AssertGet(t, c, go_client.GetRequest{Key: "counter"}, "7", nil, false)
}
