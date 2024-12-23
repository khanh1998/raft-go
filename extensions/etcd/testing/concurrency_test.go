package testing

import (
	"khanh/raft-go/extensions/etcd/go_client"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWith5Clients(t *testing.T) {
	client := 5
	target := 10
	os.RemoveAll("data/")
	c := NewCluster("config/3-nodes.yml")
	err := c.InitConcurrencyTest(client)
	assert.NoError(t, err)
	//defer c.Clean()
	AssertHavingOneLeader(t, c)

	wg := sync.WaitGroup{}
	for i := 0; i < client; i++ {
		wg.Add(1)
		go func(t *testing.T, c *Cluster) {
			i := 0
			for {
				err = IncreaseByOneC(t, c, "counter")
				if err == nil {
					i++
				} else {
					t.Logf("IncreaseByOneC: %s", err.Error())
				}

				if i == target {
					break
				}
			}
			wg.Done()
		}(t, c)
	}

	wg.Wait()

	AssertGet(t, c, go_client.GetRequest{Key: "counter"}, strconv.Itoa(client*target-1), nil, false)
}
