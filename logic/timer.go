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
		case <-n.electionTimeOut.C:
			// Thus, a leader in Raft steps down if an election timeout elapses without a successful round of heartbeats to a majority of its cluster;
			// this allows clients to retry their requests with another server.
			// TODO: brings this out of the loop
			if n.state == common.StateLeader && !majorityOK {
				n.toFollower()
				n.setLeaderID(0)
				n.setVotedFor(0)

				n.log().Debug().Msg("main loop: leader step down")
			}
			majorityOK = false
			n.BroadCastRequestVote()
		case <-n.heartBeatTimeOut.C:
			majorityOK = n.BroadcastAppendEntries() || majorityOK
		case <-n.stop:
			stop = true
		}
	}
}

func (n *RaftBrainImpl) resetElectionTimeout() {
	n.dataLock.Lock()
	defer n.dataLock.Unlock()

	randomElectionTimeOut := time.Duration(common.RandInt(n.electionTimeOutMin, n.electionTimeOutMax)) * time.Millisecond
	n.log().Info().Interface("seconds", randomElectionTimeOut.Seconds()).Msg("resetElectionTimeout")
	if n.electionTimeOut == nil {
		n.electionTimeOut = time.NewTimer(randomElectionTimeOut)
	} else {
		n.electionTimeOut.Reset(randomElectionTimeOut)
	}
}

func (n *RaftBrainImpl) resetHeartBeatTimeout() {
	n.dataLock.Lock()
	defer n.dataLock.Unlock()

	randomHeartBeatTimeout := time.Duration(common.RandInt(n.heartBeatTimeOutMin, n.heartBeatTimeOutMax)) * time.Millisecond
	n.log().Info().Interface("seconds", randomHeartBeatTimeout.Seconds()).Msg("resetHeartBeatTimeout")
	if n.heartBeatTimeOut == nil {
		n.heartBeatTimeOut = time.NewTimer(randomHeartBeatTimeout)
	} else {
		n.heartBeatTimeOut.Reset(randomHeartBeatTimeout)
	}
}
