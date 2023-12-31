package logic

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
			if n.State == StateLeader && !majorityOK {
				n.ToFollower()

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
