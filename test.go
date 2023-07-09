package main

import (
	"khanh/raft-go/logic"
	"time"

	"github.com/rs/zerolog/log"
)

type NodeImpl struct {
	ID              int
	ElectionTimeOut chan time.Time
}

func (n *NodeImpl) resetElectionTimeout() {
	randomElectionTimeOut := time.Duration(logic.RandInt(5000, 10000)) * time.Millisecond
	dur := randomElectionTimeOut * 2
	log.Info().Interface("second", dur.Seconds()).Msg("resetElectionTimeout")
	go func() {
		<-time.After(dur)
		n.ElectionTimeOut <- time.Now()
		log.Info().
			Interface("second", dur.Seconds()).Msg("resetElectionTimeout: done")
	}()
}

func main() {
	log.Info().Msg("start")
	t := time.NewTimer(1 * time.Second)
	<-t.C
	t.Reset(10 * time.Second)
	<-t.C
	log.Info().Msg("start")
}
