package integration_testing

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
)

func random(min, max int, unit time.Duration) time.Duration {
	rand.Seed(uint64(time.Now().UnixNano()))
	r := int64(rand.Intn(max-min+1) + min)
	return time.Duration(r * unit.Nanoseconds())
}

// This test demonstrates how you can use the key-locking feature to coordinate multiple concurrent processes taking turns and working together.
func TestKeyLocking(t *testing.T) {
	os.RemoveAll("data/")
	c := NewCluster("config/3-nodes.yml")
	defer c.Clean()

	AssertHavingOneLeader(t, c)

	serverInfos := c.HttpAgent.serverInfos

	agents := []*HttpAgent{}
	agentCount := 3

	for i := 0; i < agentCount; i++ {
		agent := NewHttpAgent(HttpAgentArgs{serverUrls: serverInfos, Log: c.log})
		agents = append(agents, agent)
	}

	wg := sync.WaitGroup{}
	for i := 0; i < agentCount; i++ {
		wg.Add(1)
		go func(ag *HttpAgent) {
			err := ag.RegisterClient()
			assert.NoError(t, err)
			localCount, localTarget := 0, 5
			initCounter := false

			for localCount < localTarget {
				// acquire the lock
				err := ag.ClientRequest("set", true, "my-turn", strconv.Itoa(ag.clientId))
				if err != nil {
					ag.log.Error("ClientRequest set my-turn", err, "clientId", ag.clientId)

					dur := random(50, 300, time.Millisecond)
					time.Sleep(dur)
					ag.log.Info("sleep", "duration", dur, "clientId", ag.clientId)

					continue
				}

				// get current value of the counter
				countStr, err := ag.ClientQuery("count")
				if err != nil {
					ag.log.Error("ClientQuery count", err, "clientId", ag.clientId)

					if strings.Contains(err.Error(), "key does not exist") {
						initCounter = true
					}
				}

				// increase value of the counter,
				// set new value of counter back
				if initCounter {
					ag.ClientRequest("set", false, "count", "1")
					initCounter = false
				} else {
					count, err := strconv.Atoi(countStr)
					if err != nil {
						ag.log.Fatal("strconv.Atoi", "value", countStr)
					}

					ag.ClientRequest("set", false, "count", strconv.Itoa(count+1))
				}

				localCount += 1
				// release the lock
				err = ag.ClientRequest("del", false, "my-turn", "")
				if err != nil {
					ag.log.Error("ClientRequest del my-turn", err, "clientId", ag.clientId)
				}

				dur := random(50, 300, time.Millisecond)
				time.Sleep(dur)
				ag.log.Info("sleep", "duration", dur, "clientId", ag.clientId)
			}

			wg.Done()
		}(agents[i])
	}
	wg.Wait()

	// final count
	AssertGet(t, c, "count", "15")
}
