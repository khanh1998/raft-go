package logic

func (n *NodeImpl) loop() {
	n.log().Info().Msg("Raft main loop is started")
	for {
		select {
		case <-n.ElectionTimeOut.C:
			go n.BroadCastRequestVote()
		case <-n.HeartBeatTimeOut.C:
			go n.BroadcastAppendEntries()
		}
	}
}
