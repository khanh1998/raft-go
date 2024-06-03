package integration_testing

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCreateNodes(t *testing.T) {
	c := NewDynamicCluster("dynamic.yml")
	err := c.createNewNode(2, "localhost:1235", "locahost:8080")
	timeout := 1 * time.Second
	assert.NoError(t, err)
	res, err := c.RpcAgent.SendPing(1, &timeout)
	assert.NoError(t, err)
	assert.Equal(t, 1, res.ID)
	res, err = c.RpcAgent.SendPing(2, &timeout)
	assert.NoError(t, err)
	assert.Equal(t, 2, res.ID)
}
