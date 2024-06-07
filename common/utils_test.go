package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRandInt(t *testing.T) {
	assert.Panics(t, func() {
		RandInt(0, 0)
	})
	r := RandInt(0, 1)
	assert.Equal(t, int64(0), r)
}
