package common

const NoOperation = "NO-OP"

type AppendEntriesInput struct {
	Term         int   // leader’s term
	LeaderID     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader’s commitIndex
}

type AppendEntriesOutput struct {
	Term    int    // currentTerm, for leader to update itself
	Success bool   // true if follower contained entry matching prevLogIndex and prevLogTerm
	Message string // for debuging purpose
	NodeID  int    // id of the responder
}

type RequestVoteInput struct {
	Term         int // candidate’s term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

type RequestVoteOutput struct {
	Term        int    // currentTerm, for candidate to update itself
	VoteGranted bool   // true means candidate received vote
	Message     string // for debuging purpose
	NodeID      int    // id of the responder
}

type PeerInfo struct {
	ID  int
	URL string
}
