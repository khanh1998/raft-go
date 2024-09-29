package logic

import (
	"context"
	"khanh/raft-go/common"
	"time"

	"go.opentelemetry.io/otel"
)

var (
	tracer = otel.Tracer("raft-brain")
)

func (n *RaftBrainImpl) loop(ctx context.Context) {
	n.log().InfoContext(ctx, "Raft main loop has been started")
	stop := false
	majorityOK := false
	for {
		if stop {
			n.log().InfoContext(context.Background(), "Raft main loop has been stopped")

			break
		}

		select {
		case <-n.electionTimeOut.C:
			// Thus, a leader in Raft steps down if an election timeout elapses without a successful round of heartbeats to a majority of its cluster;
			// this allows clients to retry their requests with another server.
			// TODO: brings this out of the loop
			ctx, span := tracer.Start(context.Background(), "ElectionTimeout")
			if n.state == common.StateLeader && !majorityOK {
				n.toFollower(ctx)
				n.setLeaderID(ctx, 0)
				n.setVotedFor(ctx, 0)

				n.log().InfoContext(ctx, "Raft main loop has been stopped")
			}
			majorityOK = false
			n.BroadCastRequestVote(ctx)
			span.End()
		case <-n.heartBeatTimeOut.C:
			ctx, span := tracer.Start(context.Background(), "HeartBeatTimeout")
			majorityOK = n.BroadcastAppendEntries(ctx) || majorityOK
			span.End()
		case <-n.stop:
			stop = true
		}
	}
}

func (n *RaftBrainImpl) resetElectionTimeout(ctx context.Context) {
	n.dataLock.Lock()
	defer n.dataLock.Unlock()

	randomElectionTimeOut := time.Duration(common.RandInt(n.electionTimeOutMin, n.electionTimeOutMax)) * time.Millisecond
	n.log().InfoContext(ctx, "resetElectionTimeout", "seconds", randomElectionTimeOut.Seconds())
	if n.electionTimeOut == nil {
		n.electionTimeOut = time.NewTimer(randomElectionTimeOut)
	} else {
		n.electionTimeOut.Reset(randomElectionTimeOut)
	}
}

func (n *RaftBrainImpl) resetHeartBeatTimeout(ctx context.Context) {
	n.dataLock.Lock()
	defer n.dataLock.Unlock()

	randomHeartBeatTimeout := time.Duration(common.RandInt(n.heartBeatTimeOutMin, n.heartBeatTimeOutMax)) * time.Millisecond
	n.log().InfoContext(ctx, "resetHeartBeatTimeout", "seconds", randomHeartBeatTimeout.Seconds())
	if n.heartBeatTimeOut == nil {
		n.heartBeatTimeOut = time.NewTimer(randomHeartBeatTimeout)
	} else {
		n.heartBeatTimeOut.Reset(randomHeartBeatTimeout)
	}
}
