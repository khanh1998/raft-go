package logic

func (n *NodeImpl) loop() {
	n.log().Info().Msg("Raft main loop is started")
	for {
		select {
		case <-n.ElectionTimeOut.C:
			if len(n.Peers) >= n.Quorum {
				go n.BroadCastRequestVote()
			}
		case <-n.HeartBeatTimeOut.C:
			go n.BroadcastAppendEntries()
		case req := <-n.ClientRequests:
			go func() {
				n.log().Info().Interface("request", req).Msg("[main loop]received new request from client")
				n.AppendLog(Log{
					Term:   n.CurrentTerm,
					Values: req.Data,
				})

				if n.State == StateLeader {
					n.resetHeartBeatTimeout()
				} else {
					// TODO: redirect to leader somehow
				}
			}()
		}
	}
}
