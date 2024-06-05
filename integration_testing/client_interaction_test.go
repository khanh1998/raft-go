package integration_testing

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithoutLeaderCrashed(t *testing.T) {
	c := NewCluster("config/3-nodes.yml")
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

func TestCoutingInStaticCluster(t *testing.T) {
	c := NewCluster("config/3-nodes.yml")
	defer c.Clean()
	AssertHavingOneLeader(t, c)
	AssertClientRegister(t, c)
	for i := 0; i < 10; i++ {
		IncreaseByOne(t, c, "count")
	}
	AssertGet(t, c, "count", "10")
}

func TestWithLeaderCrashed(t *testing.T) {
	c := NewCluster("config/3-nodes.yml")
	defer c.Clean()

	AssertHavingOneLeader(t, c)
	AssertLiveNode(t, c, 3)
	AssertClientRegister(t, c)
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
