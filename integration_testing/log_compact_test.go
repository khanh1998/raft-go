package integration_testing

import "testing"

func TestSnapshotTaking(t *testing.T) {
	c := NewCluster("config/3-nodes.yml")
	defer c.Clean()
	AssertHavingOneLeader(t, c)
	IncreaseBy(t, c, "count", 10)
}
