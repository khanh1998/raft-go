package logic

import (
	"khanh/raft-go/common"
	"time"
)

func (n *RaftBrainImpl) loop() {
	n.log().Info().Msg("Raft main loop has been started")
	stop := false
	majorityOK := false
	for {
		if stop {
			n.log().Info().Msg("Raft main loop has been stopped")

			break
		}

		select {
		case <-n.ElectionTimeOut.C:
			// Thus, a leader in Raft steps down if an election timeout elapses without a successful round of heartbeats to a majority of its cluster;
			// this allows clients to retry their requests with another server.
			// TODO: brings this out of the loop
			if n.State == common.StateLeader && !majorityOK {
				n.toFollower()
				n.setLeaderID(0)
				n.setVotedFor(0)

				n.log().Debug().Msg("main loop: leader step down")
			}
			majorityOK = false
			n.BroadCastRequestVote()
		case <-n.HeartBeatTimeOut.C:
			majorityOK = n.BroadcastAppendEntries() || majorityOK
		case <-n.Stop:
			stop = true
		}
	}
}

func (n *RaftBrainImpl) resetElectionTimeout() {
	randomElectionTimeOut := time.Duration(common.RandInt(n.ElectionTimeOutMin, n.ElectionTimeOutMax)) * time.Millisecond
	n.log().Info().Interface("seconds", randomElectionTimeOut.Seconds()).Msg("resetElectionTimeout")
	if n.ElectionTimeOut == nil {
		n.ElectionTimeOut = time.NewTimer(randomElectionTimeOut)
	} else {
		n.ElectionTimeOut.Reset(randomElectionTimeOut)
	}
}

func (n *RaftBrainImpl) resetHeartBeatTimeout() {
	randomHeartBeatTimeout := time.Duration(common.RandInt(n.HeartBeatTimeOutMin, n.HeartBeatTimeOutMax)) * time.Millisecond
	n.log().Info().Interface("seconds", randomHeartBeatTimeout.Seconds()).Msg("resetHeartBeatTimeout")
	if n.HeartBeatTimeOut == nil {
		n.HeartBeatTimeOut = time.NewTimer(randomHeartBeatTimeout)
	} else {
		n.HeartBeatTimeOut.Reset(randomHeartBeatTimeout)
	}
}
