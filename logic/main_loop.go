package logic

func (n *RaftBrainImpl) loop() {
	n.log().Info().Msg("Raft main loop is started")
	for {
		select {
		case <-n.ElectionTimeOut.C:
			go n.BroadCastRequestVote()
		case <-n.HeartBeatTimeOut.C:
			// Thus, a leader in Raft steps down if an election timeout elapses without a successful round of heartbeats to a majority of its cluster; this allows clients to retry their requests with another server.
			go n.BroadcastAppendEntries()
		}
	}
}
