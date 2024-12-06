package common

type ClientRequestInput struct {
	ClientID    int    `json:"client_id"`
	SequenceNum int    `json:"sequence_num"`
	Command     string `json:"command"`
}

type ClientRequestOutput struct {
	Status     ClientRequestStatus `json:"status"`
	Response   LogResult           `json:"response"`
	LeaderHint string              `json:"leader_hint"`
}

type RegisterClientInput struct{}
type RegisterClientOutput struct {
	Status     ClientRequestStatus `json:"status"`
	LeaderHint string              `json:"leader_hint"`
	Response   LogResult           `json:"response,omitempty"`
}

type KeepAliveClientInput struct {
	ClientID    int `json:"client_id"`
	SequenceNum int `json:"sequence_num"`
}

type KeepAliveClientOutput struct {
	Status     ClientRequestStatus `json:"status"`
	LeaderHint string              `json:"leader_hint"`
	Response   LogResult           `json:"response,omitempty"`
}

type ClientQueryInput struct {
	Query string `json:"query"`
}

type ClientQueryOutput struct {
	Status     ClientRequestStatus `json:"status"`
	Response   LogResult           `json:"response"`
	LeaderHint string              `json:"leader_hint"`
}

type GetStatusResponse struct {
	ID          int       `json:"id"`
	State       RaftState `json:"state"`
	Term        int       `json:"term"`
	LeaderId    int       `json:"leader_id"`
	ClusterTime uint64    `json:"cluster_time"`
	CommitIndex int       `json:"commit_index"`
}
