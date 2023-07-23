package common

import (
	"crypto/rand"
	"math/big"

	"github.com/rs/zerolog/log"
)

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
