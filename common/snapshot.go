package common

import (
	"fmt"
	"regexp"
	"time"
)

type SnapshotNotification struct {
	LastConfig map[int]ClusterMember // cluster members
	LastTerm   int
	LastIndex  int
}

type Snapshot struct {
	lastConfig map[int]ClusterMember // cluster members
	data       map[string]string
	lastTerm   int
	lastIndex  int
}

func (s Snapshot) copy() Snapshot {
	lastConfig := map[int]ClusterMember{}
	data := map[string]string{}

	for k, v := range s.lastConfig {
		lastConfig[k] = v
	}

	for k, v := range s.data {
		data[k] = v
	}

	return Snapshot{
		lastTerm:   s.lastTerm,
		lastIndex:  s.lastIndex,
		lastConfig: lastConfig,
		data:       data,
	}
}

type InstallSnapshotInput struct {
	Term      int
	LeaderId  int
	LastIndex int

	LastTerm   int
	LastConfig []ClusterMember

	Offset int

	Data []byte
	Done bool
}

type InstallSnapshotOutput struct {
	Term int
}

func NewSnapshotFileName() string {
	now := time.Now().UnixNano()
	return fmt.Sprintf("snapshot.%d.dat", now)
}

func IsSnapshotFile(fileName string) bool {
	pattern := `^snapshot\.\d+\.dat$`
	regex := regexp.MustCompile(pattern)

	return regex.MatchString(fileName)
}
