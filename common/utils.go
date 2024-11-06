package common

import (
	"crypto/rand"
	"math/big"
	"strings"

	"github.com/rs/zerolog/log"
)

func CopySlice[T any](source []T) []T {
	if source == nil {
		return nil
	}
	destination := make([]T, len(source))
	copy(destination, source)
	return destination
}

func trimAndLower(s string) string {
	return strings.ToLower(strings.Trim(s, " "))
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func RandInt(min, max int64) int64 {
	diff := max - min
	nBig, err := rand.Int(rand.Reader, big.NewInt(int64(diff)))
	if err != nil {
		log.Err(err).Msg("cannot generate random number")
	}

	return min + nBig.Int64()
}
