package common

type AddServerInput struct {
	ID               int
	NewServerHttpUrl string
	NewServerRpcUrl  string
}

type AddServerOutput struct {
	Status     ClientRequestStatus `json:"status"`
	LeaderHint string              `json:"leader_hint"`
	Response   string              `json:"response"`
}

type RemoveServerInput struct {
	ID               int
	NewServerHttpUrl string
	NewServerRpcUrl  string
}

type RemoveServerOutput struct {
	Status     ClientRequestStatus `json:"status"`
	LeaderHint string              `json:"leader_hint"`
	Response   string              `json:"response"`
}
