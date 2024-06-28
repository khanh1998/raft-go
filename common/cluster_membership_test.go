package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClusterMemberToString(t *testing.T) {
	c := ClusterMember{
		ID:      1,
		RpcUrl:  "localhost:1234",
		HttpUrl: "localhost:8080",
	}
	assert.Equal(t, "1|localhost:8080|localhost:1234", c.ToString())

	d := ClusterMember{}
	err := d.FromString(c.ToString())

	assert.NoError(t, err)
	assert.Equal(t, c, d)
}

func TestClusterMemberFromString(t *testing.T) {
	c := ClusterMember{
		ID:      1,
		RpcUrl:  "localhost:1234",
		HttpUrl: "localhost:8080",
	}

	d := ClusterMember{}
	err := d.FromString("1|localhost:8080|localhost:1234")
	assert.NoError(t, err)
	assert.Equal(t, c, d)

	d = ClusterMember{}
	err = d.FromString("aaaa|localhost:8080|localhost:1234")
	assert.Error(t, err)

	d = ClusterMember{}
	err = d.FromString("aaaa|localhost:8080|localhost:1234|localhost:80")
	assert.Error(t, err)

	d = ClusterMember{}
	err = d.FromString("aaaa|localhost:8080")
	assert.Error(t, err)

	d = ClusterMember{}
	err = d.FromString("")
	assert.Error(t, err)
}
