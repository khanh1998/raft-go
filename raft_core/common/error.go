package common

import "errors"

var (
	// state machine
	ErrorSessionExpired = errors.New("session expired: no record of session can be found")
	ErrLogIsEmpty       = errors.New("log is empty")
	ErrIndexOutOfRange  = errors.New("index out of range")
	ErrLogIsInSnapshot  = errors.New("log is in snapshot")
	ErrEmptyData        = errors.New("data is empty")
)
