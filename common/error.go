package common

type RaftError struct {
	HttpCode   int
	LeaderHint string
	Message    string
}

func (r RaftError) Error() string {
	return r.Message
}
