package common

type AddServerInput struct {
	ID               int    `json:"id"`
	NewServerHttpUrl string `json:"http_url"`
	NewServerRpcUrl  string `json:"rpc_url"`
}

type AddServerOutput struct {
	Status     ClientRequestStatus `json:"status"`
	LeaderHint string              `json:"leader_hint"`
}

type RemoveServerInput struct {
	ID int `json:"id"`
}

type RemoveServerOutput struct {
	Status     ClientRequestStatus `json:"status"`
	LeaderHint string              `json:"leader_hint"`
}
