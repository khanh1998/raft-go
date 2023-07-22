package logic

import "khanh/raft-go/common"

func (n *NodeImpl) loop() {
	n.log().Info().Msg("Raft main loop is started")
	for {
		select {
		case <-n.ElectionTimeOut.C:
			go n.BroadCastRequestVote()
		case <-n.HeartBeatTimeOut.C:
			go n.BroadcastAppendEntries()
		case req := <-n.ClientRequests:
			go func() {
				// If command received from client: append entry to local log,
				// respond after entry applied to state machine (§5.3)

				if n.State == StateLeader {
					n.AppendLog(common.Log{
						Term:   n.CurrentTerm,
						Values: req.Data,
					})
				} else {
					n.log().Info().Msg("follower can not process client request for now")
				}
			}()
		}
	}
}
