package common

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

func GetPointer[T any](input T) *T {
	return &input
}

func CopySlice[T any](source []T) []T {
	if source == nil {
		return nil
	}
	destination := make([]T, len(source))
	copy(destination, source)
	return destination
}

func TrimAndLower(s string) string {
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

func NewSnapshotFileName(term, index int) string {
	return fmt.Sprintf("snapshot.%020d_%020d.dat", term, index)
}

func IsSnapshotFile(fileName string) bool {
	pattern := `^snapshot\.(\d+)_(\d+)\.dat$`
	regex := regexp.MustCompile(pattern)

	return regex.MatchString(fileName)
}

func IsTmpSnapshotFile(fileName string) bool {
	pattern := `^tmp.snapshot\.\d+_\d+\.dat$`
	regex := regexp.MustCompile(pattern)

	return regex.MatchString(fileName)
}

func NewWalFileName() string {
	now := time.Now().UnixNano()
	return fmt.Sprintf("snapshot.%d.dat", now)
}

func IsWalFile(fileName string) bool {
	pattern := `^wal\.\d+\.dat$`
	regex := regexp.MustCompile(pattern)

	return regex.MatchString(fileName)
}
