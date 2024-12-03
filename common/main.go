package common

const NoOperation = "NO-OP"
const TimeCommit = "time-commit"

type RaftState string

const (
	StateFollower   RaftState = "follower"
	StateCandidate  RaftState = "candidate"
	StateLeader     RaftState = "leader"
	StateCatchingUp RaftState = "catching-up" // new node is catching up with current leader of the cluster, it can't vote
	StateRemoved    RaftState = "removed"     // got removed from cluster
)

func (s RaftState) String() string {
	return string(s)
}

type ClientRequestStatus string

const (
	StatusOK    ClientRequestStatus = "OK"
	StatusNotOK ClientRequestStatus = "Not OK"

	NotLeader      string = "NOT_LEADER"
	SessionExpired string = "SESSION_EXPIRED"
)
