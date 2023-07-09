package logic

import "errors"

var (
	ErrLogIsEmtpy      = errors.New("log is empty")
	ErrIndexOutOfRange = errors.New("index out of range")
)
