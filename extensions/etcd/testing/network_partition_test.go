package testing

import (
	"context"
	gc "khanh/raft-go/common"
	"khanh/raft-go/extensions/etcd/go_client"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLeaderElection(t *testing.T) {
	c := NewCluster("config/3-nodes.yml")
	defer c.Clean()
	AssertHavingOneLeader(t, c)
}

func TestCurrentLeaderGotIsolated(t *testing.T) {
	// the current leader got isolated,
	// the other followers elect new leader,
	// but the previous leader still think it's leader for a while

	os.RemoveAll("data/")
	c := NewCluster("config/3-nodes-ns.yml")

	l1 := AssertHavingOneLeader(t, c)
	AssertSet(t, c, go_client.SetRequest{Key: "counter", Value: gc.GetPointer("1")})
	_ = l1
	c.IsolateNode(l1.ID)
	time.Sleep(c.config.RaftCore.MinElectionTimeout)
	l2 := AssertHavingOneLeader(t, c)
	_, err := c.HttpAgent.Get(context.Background(), go_client.GetRequest{Key: "counter", Target: &go_client.SelectedNode{
		NodeId: l1.ID,
	}})
	assert.Error(t, err)

	AssertSet(t, c, go_client.SetRequest{Key: "counter", Value: gc.GetPointer("2"), PrevExist: gc.GetPointer(true), PrevValue: gc.GetPointer("1")})

	assert.NotEqual(t, l2.ID, l1.ID)
	c.RevertIsolateNode(l1.ID)

	AssertIncreaseCounter(t, c, "counter", 1)

	c.IsolateNode(l2.ID)
}

func TestCurrentLeaderGotIsolated2(t *testing.T) {
	// the current leader got isolated,
	// the other followers elect new leader,
	// but the previous leader still think it's leader for a while

	os.RemoveAll("data/")
	c := NewCluster("config/3-nodes-ns.yml")
	l1 := AssertHavingOneLeader(t, c)
	AssertIncreaseCounter(t, c, "counter", 5)

	time.Sleep(c.MaxElectionTimeout) // wait for log committed
	// counter = 5
	c.IsolateNode(l1.ID)
	time.Sleep(c.MaxElectionTimeout) // wait for the isolated leader l1 steps down

	l2 := AssertHavingOneLeader(t, c)
	assert.NotEqual(t, l1.ID, l2.ID)
	assert.Greater(t, l2.Term, l1.Term)
	AssertIncreaseCounter(t, c, "counter", 5)

	c.RevertIsolateNode(l1.ID) // l1 with higher term (but outdated logs) will disrupt the cluster

	c.IsolateNode(l2.ID)
	time.Sleep(c.MaxElectionTimeout) // wait for the isolated leader l2 steps down

	time.Sleep(3 * c.MaxElectionTimeout) // need little more time so l1 can catchup with the cluster
	l3 := AssertHavingOneLeader(t, c)
	// counter = 10
	assert.NotEqual(t, l2.ID, l3.ID)
	assert.NotEqual(t, l1.ID, l3.ID) // l1's logs are outdated, it can't become leader
	AssertIncreaseCounter(t, c, "counter", 5)
	time.Sleep(c.MaxElectionTimeout) // wait so l3 replicates logs to l1
	// counter = 15

	c.RevertIsolateNode(l2.ID)           // l2 with higher term will disrupt the cluster, trigger election
	time.Sleep(1 * c.MaxElectionTimeout) // wait for the isolated leader l3 steps down
	time.Sleep(3 * c.MaxElectionTimeout) // wait for log replication
	l4 := AssertHavingOneLeader(t, c)
	assert.NotEqual(t, l4.ID, l2.ID)
	AssertIncreaseCounter(t, c, "counter", 5)
	AssertGet(t, c, go_client.GetRequest{Key: "counter"}, "20", nil, false)
}

func TestNetworkPartition(t *testing.T) {
	os.RemoveAll("data/")
	c := NewCluster("config/5-nodes.yml")
	_ = c
}

func TestLogsConflict(t *testing.T) {
	// submit a lot of logs to the leader so that it can't keep up to replicate logs to followers,
	// then immediately isolate the leader, the cluster then promotes a follower to become new leader,
	// make the old leader available again, there will be some log conflict
	os.RemoveAll("data/")
	c := NewCluster("config/3-nodes-ns.yml")
	l1 := AssertHavingOneLeader(t, c)
	for i := 0; i < 10; i++ {
		value := strconv.Itoa(i)
		c.HttpAgent.Set(context.Background(), go_client.SetRequest{Key: "counter", Value: gc.GetPointer(value), IgnoreResponse: true})
	}

	time.Sleep(1 * c.MaxElectionTimeout) // wait for some logs get committed
	c.IsolateNode(l1.ID)
	time.Sleep(c.MaxElectionTimeout) // wait leader l1 steps down

	l2 := AssertHavingOneLeader(t, c)
	for i := 10; i < 20; i++ {
		value := strconv.Itoa(i)
		c.HttpAgent.Set(context.Background(), go_client.SetRequest{Key: "counter", Value: gc.GetPointer(value), IgnoreResponse: true})
	}

	c.RevertIsolateNode(l1.ID)
	time.Sleep(1 * c.MaxElectionTimeout) // wait for some logs get committed
	c.IsolateNode(l2.ID)
	time.Sleep(c.MaxElectionTimeout) // wait leader l2 steps down
	l3 := AssertHavingOneLeader(t, c)
	_ = l3
	for i := 20; i < 30; i++ {
		value := strconv.Itoa(i)
		c.HttpAgent.Set(context.Background(), go_client.SetRequest{Key: "counter", Value: gc.GetPointer(value), IgnoreResponse: true})
	}
	c.RevertIsolateNode(l2.ID)
	time.Sleep(3 * c.MaxElectionTimeout) // wait for log committed
}

func TestIsolatedLeaderDisturb(t *testing.T) {
	// when a leader get isolated for a long time,
	// it keeps increasing the term and trigger election,
	// results in it will have larger term than the rest of cluster.
	// when the isolated leader rejoin the cluster, it will disrupt the current state,
	// trigger a new round of election since it got a higher term.
	os.RemoveAll("data/")
	c := NewCluster("config/3-nodes-ns.yml")
	l1 := AssertHavingOneLeader(t, c)
	c.IsolateNode(l1.ID)

	time.Sleep(c.MaxElectionTimeout) // wait for l1 to step down
	l2 := AssertHavingOneLeader(t, c)
	assert.NotEqual(t, l1.ID, l2.ID)
	assert.Greater(t, l2.Term, l1.Term)

	AssertIncreaseCounter(t, c, "counter", 5)

	time.Sleep(3 * c.MaxElectionTimeout) // give l1 a little time to increase term and request vote
	l1Info, err := c.HttpAgent.GetInfo(context.Background(), l1.ID)
	assert.NoError(t, err)

	c.RevertIsolateNode(l1.ID)
	time.Sleep(3 * c.MaxElectionTimeout) // l1 has higher term, will disrupt the cluster, trigger leader election

	l3 := AssertHavingOneLeader(t, c)
	assert.NotEqual(t, l1.ID, l3.ID)               // l1's logs are out of sync, can't be the next leader
	assert.GreaterOrEqual(t, l3.Term, l1Info.Term) // l2 will adopt l1 term
}

func TestTwoLeaders(t *testing.T) {
	// when we isolate the current leader, a follower will be choose to be the next leader in next term,
	// sometime both old leader (previous term) and new leader (current term) can coexist,
	// request to old leader should be blocked.
	os.RemoveAll("data/")
	c := NewCluster("config/3-nodes-ns.yml")
	l1 := AssertHavingOneLeader(t, c)
	AssertSet(t, c, go_client.SetRequest{Key: "counter", Value: gc.GetPointer("1")})
	c.IsolateNode(l1.ID)
	// immediately send request to isolated leader,
	// the isolated leader still haven't aware that it got isolated.
	_, err := c.HttpAgent.Get(context.Background(), go_client.GetRequest{Key: "counter", Target: &go_client.SelectedNode{NodeId: l1.ID}})
	assert.Error(t, err)
	c.RevertIsolateNode(l1.ID) // we got three nodes cluster back

	l2 := AssertHavingOneLeader(t, c)

	AssertSet(t, c, go_client.SetRequest{Key: "counter", Value: gc.GetPointer("2"), PrevValue: gc.GetPointer("1")})
	c.IsolateNode(l2.ID)
	time.Sleep(c.MaxElectionTimeout) // wait util the isolated leader steps down
	l3 := AssertHavingOneLeader(t, c)
	_ = l3
	// send request to leader l2, which now is a follower
	_, err = c.HttpAgent.Get(context.Background(), go_client.GetRequest{Key: "counter", Target: &go_client.SelectedNode{NodeId: l2.ID}})
	assert.Error(t, err)
}
