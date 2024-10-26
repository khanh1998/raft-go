package logic

import "errors"

var (
	ErrLogIsEmpty      = errors.New("log is empty")
	ErrIndexOutOfRange = errors.New("index out of range")
)
