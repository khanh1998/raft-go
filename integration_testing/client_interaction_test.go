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
		AssertSetAndGet(t, c, key, value)
	}
}

func AssertGet(t *testing.T, c *Cluster, key, expectedValue string) {
	value, err := c.HttpAgent.ClientQuery(key)
	assert.NoError(t, err)
	assert.Equal(t, expectedValue, value)
}

func AssertSetAndGetRange(t *testing.T, c *Cluster, key string, from int, to int) {
	for i := from; i < to; i++ {
		value := strconv.Itoa(i)
		AssertSetAndGet(t, c, key, value)
	}
}

func IncreaseByOne(t *testing.T, c *Cluster, key string) {
	value, err := c.HttpAgent.ClientQuery(key)
	if err != nil {
		err = c.HttpAgent.ClientRequest(key, "0")
		assert.NoError(t, err)
		value, err = c.HttpAgent.ClientQuery(key)
	}
	assert.NoError(t, err)
	intVal, err := strconv.Atoi(value)
	assert.NoError(t, err)
	nextVal := strconv.Itoa(intVal + 1)
	err = c.HttpAgent.ClientRequest(key, nextVal)
	assert.NoError(t, err)
}

func AssertSetAndGet(t *testing.T, c *Cluster, key, value string) {
	setValue := value

	seqNum := c.HttpAgent.sequenceNum
	err := c.HttpAgent.ClientRequest("count", setValue)
	assert.NoError(t, err)

	getValue, err := c.HttpAgent.ClientQuery("count")
	assert.NoError(t, err)
	assert.Equal(t, setValue, getValue)
	assert.Greater(t, c.HttpAgent.sequenceNum, seqNum)
}

func AssertRegister(t *testing.T, c *Cluster) {
	assert.Equal(t, c.HttpAgent.clientId, 0)
	err := c.HttpAgent.RegisterClient()
	assert.NoError(t, err)
	assert.Greater(t, c.HttpAgent.clientId, 0)
}

func TestCoutingInStaticCluster(t *testing.T) {
	c := NewCluster("3-nodes.yml")
	defer c.Clean()
	AssertHavingOneLeader(t, c)
	AssertRegister(t, c)
	for i := 0; i < 10; i++ {
		IncreaseByOne(t, c, "count")
	}
	AssertGet(t, c, "count", "10")
}

func AssertLiveNode(t *testing.T, c *Cluster, expectCount int) {
	count, err := c.CountLiveNode()
	assert.NoError(t, err)
	assert.Equal(t, expectCount, count)
}

func TestWithLeaderCrashed(t *testing.T) {
	c := NewCluster("3-nodes.yml")
	defer c.Clean()

	AssertHavingOneLeader(t, c)
	AssertLiveNode(t, c, 3)
	AssertRegister(t, c)
	IncreaseByOne(t, c, "count")

	stopNodeId, err := c.StopLeader()
	assert.NoError(t, err)

	AssertLiveNode(t, c, 2)
	AssertHavingOneLeader(t, c)

	AssertGet(t, c, "count", "1")
	IncreaseByOne(t, c, "count")
	AssertGet(t, c, "count", "2")

	c.StartNode(stopNodeId)
	AssertLiveNode(t, c, 3)
}
