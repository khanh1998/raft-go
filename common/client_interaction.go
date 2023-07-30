package common

import "net/url"

type ClientRequestStatus string

const (
	StatusOK ClientRequestStatus = "OK"

	NotLeader      string = "NOT_LEADER"
	SessionExpired string = "SESSION_EXPIRED"
)

type ClientRequestInput struct {
	ClientID    int `json:"client_id"`
	SequenceNum int `json:"sequence_num"`
	Command     any `json:"command"`
}

type ClientRequestOutput struct {
	Status     ClientRequestStatus `json:"status"`
	Response   any                 `json:"response"`
	LeaderHint url.URL             `json:"leader_hint"`
}

type RegisterClientInput struct{}
type RegisterClientOutput struct {
	Status     ClientRequestStatus `json:"status"`
	ClientID   int                 `json:"client_id"`
	LeaderHint url.URL             `json:"leader_hint"`
}

type ClientQueryInput struct {
	Query any `json:"query"`
}

type ClientQueryOutput struct {
	Status     ClientRequestStatus `json:"status"`
	Response   any                 `json:"response"`
	LeaderHint url.URL             `json:"leader_hint"`
}
