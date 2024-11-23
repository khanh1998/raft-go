package common

type LogResult interface{}

// we will inject a sample concrete log to raft brain,
// so it can call for example `CreateNoOp()` to create an internal NO-OP log.
type Log interface {
	Serialize() []byte
	ToString() string
	GetTerm() int
	GetTime() uint64
	DecomposeChangeSeverCommand() (addition bool, serverId int, httpUrl string, rpcUrl string, err error)
}

// Raft brain module need to create some logs internally,
// but it don't what implementation of Log to use.
type LogFactory interface {
	AttachTermAndTime(log Log, term int, time uint64) (Log, error)
	Empty() Log
	Deserialize([]byte) (log Log, err error)
	FromString(string) (log Log, err error)
	// after a new leader elected, it will append a NO-OP log
	NoOperation(term int, time uint64) Log
	// for dynamic cluster,
	// to add a new node (server, member) to the cluster
	AddNewNode(term int, time uint64, nodeId int, httpUrl string, rpcUrl string) Log
	// to remove a node from a cluster
	RemoveNode(term int, time uint64, nodeId int, httpUrl string, rpcUrl string) Log
	// periodically the brain need to commit time,
	CreateTimeCommit(term int, nanosecond uint64) Log
}
