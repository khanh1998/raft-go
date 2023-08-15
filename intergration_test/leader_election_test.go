package intergration_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLeaderElection(t *testing.T) {
	c := NewCluster(5)
	defer c.Clean()

	time.Sleep(1000 * time.Millisecond)
	status, err := c.HasOneLeader()

	assert.NoError(t, err, "expect one leader int the cluster")
	assert.Equal(t, 1, status.Term)
}

func TestReLeaderElection(t *testing.T) {
	c := NewCluster(5)
	defer c.Clean()

	time.Sleep(1000 * time.Millisecond)
	status, err := c.HasOneLeader()

	assert.NoError(t, err, "expect one leader int the cluster")
	assert.Equal(t, 1, status.Term)

	// stop the leader
}
