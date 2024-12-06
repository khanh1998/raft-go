package testing

import (
	gc "khanh/raft-go/common"
	"khanh/raft-go/extensions/etcd/go_client"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetClearedEventHistory(t *testing.T) {
	c := NewCluster("config/3-nodes.yml")
	defer c.Clean()
	AssertHavingOneLeader(t, c)

	// in the test config, event history cache can store max 10 most recent events,
	// if user wait on an index that get cleared out from the cache, he will receive an error.
	// but user is allowed to wait on a future index.

	var prevValue string
	var modifiedIndex int
	go AssertGet(t, c, go_client.GetRequest{Key: "counter", Wait: true, WaitIndex: 15}, "14", nil, false) // wait on future index
	for i := 0; i <= 20; i++ {
		value := strconv.FormatInt(int64(i), 10)
		if i == 0 {
			_, modifiedIndex = AssertSet(t, c, go_client.SetRequest{Key: "counter", Value: &value, PrevExist: gc.GetPointer(false)})
		} else {
			_, modifiedIndex = AssertSet(t, c, go_client.SetRequest{
				Key: "counter", Value: &value, PrevExist: gc.GetPointer(true), PrevValue: &prevValue, PrevIndex: modifiedIndex,
			})
		}
		prevValue = value
	}
	AssertGet(t, c, go_client.GetRequest{Key: "counter"}, "20", nil, false)
	AssertGet(t, c, go_client.GetRequest{Key: "counter", Wait: true, WaitIndex: modifiedIndex - 1}, "19", nil, false)
	AssertGet(t, c, go_client.GetRequest{Key: "counter", Wait: true, WaitIndex: 1}, "", nil, true) // event history size is 10, index 1 is cleared
	// we can wait on x-etcd-index
}

func TestNotifyExpiredKey(t *testing.T) {
	c := NewCluster("config/3-nodes.yml")
	defer c.Clean()
	AssertHavingOneLeader(t, c)

	// when user wait on a key, if the key is expired and get removed, the user should receive a notification

	ttl := 60 * time.Second
	AssertSet(t, c, go_client.SetRequest{Key: "counter", Value: gc.GetPointer("1"), TTL: ttl.String()})
	go AssertGet(t, c, go_client.GetRequest{Key: "counter", Wait: true}, "", nil, false) // will see the delete expired key
	ttl = 1 * time.Second
	AssertSet(t, c, go_client.SetRequest{Key: "counter", Value: nil, TTL: ttl.String(), Refresh: true})
	time.Sleep(ttl)
	AssertSet(t, c, go_client.SetRequest{Key: "name", Value: gc.GetPointer("khanh")}) // update time to state machine, trigger deleting expired key
}

func TestGetAndWait(t *testing.T) {
	c := NewCluster("config/3-nodes.yml")
	defer c.Clean()
	AssertHavingOneLeader(t, c)

	go AssertGet(t, c, go_client.GetRequest{Key: "name", Wait: true}, "khanh", nil, false)
	go AssertGet(t, c, go_client.GetRequest{Key: "name", Wait: true}, "khanh", nil, false)
	go AssertGet(t, c, go_client.GetRequest{Key: "namee", Wait: true, WaitIndex: 3}, "bui", nil, false)

	AssertGet(t, c, go_client.GetRequest{Key: "age"}, "26", nil, true)

	AssertSet(t, c, go_client.SetRequest{Key: "name", Value: gc.GetPointer("khanh")})
	AssertSet(t, c, go_client.SetRequest{Key: "age", Value: gc.GetPointer("26")})
	AssertSet(t, c, go_client.SetRequest{Key: "nameee", Value: gc.GetPointer("bui")})
}

func TestGetPrefix(t *testing.T) {
	c := NewCluster("config/3-nodes.yml")
	defer c.Clean()
	AssertHavingOneLeader(t, c)

	go AssertGet(t, c, go_client.GetRequest{Key: "app", Prefix: true, Wait: true}, "", nil, true) // can't wait on prefix
	go AssertGet(t, c, go_client.GetRequest{Key: "apology", Wait: true}, "sorry", nil, false)     // cat wait on key only

	AssertSet(t, c, go_client.SetRequest{Key: "asia", Value: gc.GetPointer("asia")})
	AssertSet(t, c, go_client.SetRequest{Key: "application", Value: gc.GetPointer("vscode")})
	AssertSet(t, c, go_client.SetRequest{Key: "apply", Value: gc.GetPointer("uni")})
	AssertSet(t, c, go_client.SetRequest{Key: "apology", Value: gc.GetPointer("sorry")})
	AssertSet(t, c, go_client.SetRequest{Key: "apt", Value: gc.GetPointer("apt")})
	AssertSet(t, c, go_client.SetRequest{Key: "apple", Value: gc.GetPointer("juice")})
	AssertSet(t, c, go_client.SetRequest{Key: "app", Value: gc.GetPointer("dev")})

	AssertGet(t, c, go_client.GetRequest{Key: "app", Prefix: true}, "", []string{"vscode", "uni", "juice", "dev"}, false)
}

func TestEventHistory(t *testing.T) {
	c := NewCluster("config/3-nodes.yml")
	defer c.Clean()
	AssertHavingOneLeader(t, c)

	AssertGet(t, c, go_client.GetRequest{Key: "counter"}, "", nil, true)

	createdIndex, _ := AssertSet(t, c, go_client.SetRequest{Key: "counter", Value: gc.GetPointer("1")})
	AssertSet(t, c, go_client.SetRequest{Key: "counter", Value: gc.GetPointer("2")})

	go AssertGet(t, c, go_client.GetRequest{Key: "counter"}, "2", nil, false)
	go AssertGet(t, c, go_client.GetRequest{Key: "counter", Wait: true, WaitIndex: createdIndex}, "1", nil, false)
	go AssertGet(t, c, go_client.GetRequest{Key: "counter", Wait: true, WaitIndex: createdIndex + 1}, "2", nil, false)

}

func TestRefreshTTL(t *testing.T) {
	os.RemoveAll("data/")
	c := NewCluster("config/3-nodes.yml")
	defer c.Clean()
	AssertHavingOneLeader(t, c)

	createdIndex, _ := AssertSet(t, c, go_client.SetRequest{Key: "counter", Value: gc.GetPointer("1"), TTL: "10s"})
	go AssertGet(t, c, go_client.GetRequest{Key: "counter", Wait: true, WaitIndex: createdIndex + 1}, "2", nil, false)
	AssertSet(t, c, go_client.SetRequest{Key: "counter", Refresh: true, TTL: "1s", Value: nil})
	AssertSet(t, c, go_client.SetRequest{Key: "counter", Value: gc.GetPointer("2"), TTL: ""}) // keep the ttl (1s), only change value
	time.Sleep(time.Second)
	AssertSet(t, c, go_client.SetRequest{Key: "name", Value: gc.GetPointer("khanh")}) // purpose of this set is to update new time to state machine
	AssertGet(t, c, go_client.GetRequest{Key: "counter"}, "", nil, true)
}

func TestWithoutLeaderCrashed(t *testing.T) {
	c := NewCluster("config/3-nodes.yml")
	defer c.Clean()

	AssertHavingOneLeader(t, c)
	AssertGet(t, c, go_client.GetRequest{Key: "count"}, "", nil, true)

	for i := 0; i < 50; i++ {
		key, value := "count", strconv.Itoa(i)
		AssertSetAndGet(t, c, key, value)
	}
}

func TestCoutingInStaticCluster(t *testing.T) {
	c := NewCluster("config/3-nodes.yml")
	defer c.Clean()
	AssertHavingOneLeader(t, c)
	for i := 0; i < 10; i++ {
		IncreaseByOne(t, c, "count")
	}
	AssertGet(t, c, go_client.GetRequest{Key: "count"}, "10", nil, false)
}

func TestWithLeaderCrashed(t *testing.T) {
	c := NewCluster("config/3-nodes.yml")
	defer c.Clean()

	AssertHavingOneLeader(t, c)
	AssertLiveNode(t, c, 3)
	IncreaseByOne(t, c, "count")

	stopNodeId, err := c.StopLeader()
	assert.NoError(t, err)

	AssertLiveNode(t, c, 2)
	AssertHavingOneLeader(t, c)

	AssertGet(t, c, go_client.GetRequest{Key: "count"}, "1", nil, false)
	IncreaseByOne(t, c, "count")
	AssertGet(t, c, go_client.GetRequest{Key: "count"}, "2", nil, false)

	c.StartNode(stopNodeId)
	AssertLiveNode(t, c, 3)
}

func TestDeleteKey(t *testing.T) {
	os.RemoveAll("data/")
	c := NewCluster("config/3-nodes.yml")
	defer c.Clean()

	AssertHavingOneLeader(t, c)
	AssertLiveNode(t, c, 3)

	// simple case
	createdIndex0, _ := AssertSet(t, c, go_client.SetRequest{Key: "counter", Value: gc.GetPointer("1")})
	AssertGet(t, c, go_client.GetRequest{Key: "counter"}, "1", nil, false)
	AssertDelete(t, c, go_client.DeleteRequest{Key: "counter"}, false)
	AssertGet(t, c, go_client.GetRequest{Key: "counter"}, "", nil, true)

	// create, delete failed, delete success
	go AssertGet(t, c, go_client.GetRequest{Key: "counter", Wait: true}, "1", nil, false)
	createdIndex, _ := AssertSet(t, c, go_client.SetRequest{Key: "counter", Value: gc.GetPointer("1")})
	go AssertGet(t, c, go_client.GetRequest{Key: "counter", Wait: true, WaitIndex: createdIndex + 1}, "", nil, false)
	AssertDelete(t, c, go_client.DeleteRequest{Key: "counter", PrevExist: gc.GetPointer(false)}, true)
	AssertDelete(t, c, go_client.DeleteRequest{Key: "counter", PrevExist: gc.GetPointer(true), PrevValue: gc.GetPointer("2")}, true)
	AssertDelete(t, c, go_client.DeleteRequest{Key: "counter", PrevExist: gc.GetPointer(true), PrevIndex: createdIndex - 1}, true)
	AssertDelete(t, c, go_client.DeleteRequest{Key: "counter", PrevExist: gc.GetPointer(true), PrevValue: gc.GetPointer("1"), PrevIndex: createdIndex}, false)
	AssertGet(t, c, go_client.GetRequest{Key: "counter"}, "", nil, true)

	assert.Greater(t, createdIndex, createdIndex0)
}

func TestDeletePrefix(t *testing.T) {
	os.RemoveAll("data/")
	c := NewCluster("config/3-nodes.yml")
	defer c.Clean()

	AssertHavingOneLeader(t, c)

	AssertSet(t, c, go_client.SetRequest{Key: "asia", Value: gc.GetPointer("asia")})
	AssertSet(t, c, go_client.SetRequest{Key: "application", Value: gc.GetPointer("vscode")})
	AssertSet(t, c, go_client.SetRequest{Key: "apply", Value: gc.GetPointer("uni")})
	AssertSet(t, c, go_client.SetRequest{Key: "apology", Value: gc.GetPointer("sorry")})
	AssertSet(t, c, go_client.SetRequest{Key: "apt", Value: gc.GetPointer("apt")})
	AssertSet(t, c, go_client.SetRequest{Key: "apple", Value: gc.GetPointer("juice")})
	AssertSet(t, c, go_client.SetRequest{Key: "app", Value: gc.GetPointer("dev")})

	AssertGet(t, c, go_client.GetRequest{Key: "app", Prefix: true}, "", []string{"vscode", "uni", "juice", "dev"}, false)
	AssertDelete(t, c, go_client.DeleteRequest{Key: "app", Prefix: true, PrevExist: gc.GetPointer(true)}, true)   // can't check prevExist with delete prefix
	AssertDelete(t, c, go_client.DeleteRequest{Key: "app", Prefix: true, PrevIndex: 1}, true)                     // can't check prevIndex with delete prefix
	AssertDelete(t, c, go_client.DeleteRequest{Key: "app", Prefix: true, PrevValue: gc.GetPointer("asia")}, true) // can't check prevValue with delete prefix
	AssertDelete(t, c, go_client.DeleteRequest{Key: "app", Prefix: true}, false)
	AssertGet(t, c, go_client.GetRequest{Key: "app", Prefix: true}, "", []string{}, false)
	AssertGet(t, c, go_client.GetRequest{Key: "asia"}, "asia", nil, false)
}

func TestUploadFile(t *testing.T) {

}
