package logic

import (
	"crypto/rand"
	"math/big"

	"github.com/rs/zerolog/log"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func RandInt(min, max int64) int64 {
	diff := max - min
	nBig, err := rand.Int(rand.Reader, big.NewInt(int64(diff)))
	if err != nil {
		log.Err(err).Msg("cannot generate random number")
	}

	return min + nBig.Int64()
}

func (n *NodeImpl) lastLogInfo() (index, term int) {
	if len(n.Logs) > 0 {
		index = len(n.Logs) - 1
		term = n.Logs[index].Term

		return index + 1, term
	}

	return 0, -1
}

func (n *NodeImpl) isLogUpToDate(lastLogIndex int, lastLogTerm int) bool {
	index, term := n.lastLogInfo()
	if lastLogTerm > term {
		return true
	} else if lastLogTerm == term && lastLogIndex >= index {
		return true
	} else {
		return false
	}
}

// All servers: If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
func (n *NodeImpl) applyLog() {
	n.log().Info().
		Interface("state_machine", n.StateMachine.data).
		Interface("logs", n.Logs).
		Msg("applyLog: before")

	for n.CommitIndex > n.LastApplied {
		n.LastApplied += 1

		log, err := n.GetLog(n.LastApplied)
		if err != nil {
			break
		}

		for _, val := range log.Values {
			n.StateMachine.Put(val)
		}
	}

	n.log().Info().
		Interface("state_machine", n.StateMachine.data).
		Interface("logs", n.Logs).
		Msg("applyLog: after")
}
