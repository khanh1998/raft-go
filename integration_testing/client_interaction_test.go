package integration_testing

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithoutLeaderCrashed(t *testing.T) {
	c := NewCluster("3-nodes.yml")
	defer c.Clean()

	AssertHavingOneLeader(t, c)

	assert.Equal(t, c.HttpAgent.clientId, 0)
	err := c.HttpAgent.RegisterClient()
	assert.NoError(t, err)
	assert.Greater(t, c.HttpAgent.clientId, 0)

	value, err := c.HttpAgent.ClientQuery("count")
	assert.Error(t, err)
	assert.Equal(t, "", value)

	for i := 0; i < 50; i++ {
		key, value := "count", strconv.Itoa(i)
		SetAndGet(t, c, key, value)
	}
}

func Get(t *testing.T, c *Cluster, key, expectedValue string) {
	value, err := c.HttpAgent.ClientQuery(key)
	assert.NoError(t, err)
	assert.Equal(t, expectedValue, value)
}

func SetAndGet(t *testing.T, c *Cluster, key, value string) {
	setValue := value

	seqNum := c.HttpAgent.sequenceNum
	err := c.HttpAgent.ClientRequest("count", setValue)
	assert.NoError(t, err)

	getValue, err := c.HttpAgent.ClientQuery("count")
	assert.NoError(t, err)
	assert.Equal(t, setValue, getValue)
	assert.Greater(t, c.HttpAgent.sequenceNum, seqNum)
}

func TestWithLeaderCrashed(t *testing.T) {
	c := NewCluster("3-nodes.yml")
	defer c.Clean()

	AssertHavingOneLeader(t, c)

	assert.Equal(t, c.HttpAgent.clientId, 0)
	err := c.HttpAgent.RegisterClient()
	assert.NoError(t, err)
	assert.Greater(t, c.HttpAgent.clientId, 0)

	SetAndGet(t, c, "count", "1")
	c.StopLeader()
	AssertHavingOneLeader(t, c)

	Get(t, c, "count", "1")
	SetAndGet(t, c, "count", "2")

}
func TestClientsConcurrentlyRequest(t *testing.T) {
}
