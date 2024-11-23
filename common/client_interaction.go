package common

type ClientRequestStatus string

const (
	StatusOK    ClientRequestStatus = "OK"
	StatusNotOK ClientRequestStatus = "Not OK"

	NotLeader      string = "NOT_LEADER"
	SessionExpired string = "SESSION_EXPIRED"
)

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
