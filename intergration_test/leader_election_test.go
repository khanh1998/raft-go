package intergration_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLeaderElection(t *testing.T) {
	c := NewCluster(5)
	defer c.Clean()

	time.Sleep(3000 * time.Millisecond)
	status, err := c.HasOneLeader()

	assert.NoError(t, err, "expect one leader int the cluster")
	assert.Greater(t, status.Term, 0)
	assert.Greater(t, status.ID, 0)

}

func TestReLeaderElection(t *testing.T) {
	c := NewCluster(3)
	// defer c.Clean()

	time.Sleep(3000 * time.Millisecond)
	status1, err := c.HasOneLeader()

	assert.NoError(t, err, "expect one leader in the cluster")
	assert.Greater(t, status1.Term, 0)
	assert.Greater(t, status1.ID, 0)

	// stop the leader
	c.StopNode(status1.ID)

	_, err = c.HasOneLeader()
	assert.ErrorIs(t, err, ErrThereIsNoLeader, "expect no leader in cluster")

	// re-elect new leader
	time.Sleep(3000 * time.Millisecond)
	status3, err := c.HasOneLeader()

	assert.NoError(t, err, "expect one leader in the cluster")
	assert.GreaterOrEqual(t, status3.Term, status1.Term)
	assert.NotEqual(t, status3.ID, status1.ID)
}
