package common

import "time"

// mimic etcd 2.3 api
type EtcdLog struct {
	Term    int
	Command EtcdCommand
}

type EtcdCommand struct {
	key       []byte
	value     []byte
	ttl       time.Duration
	prevExist bool
	prevValue []byte
	prevIndex int
	refresh   bool
	wait      bool
	waitIndex int
}
