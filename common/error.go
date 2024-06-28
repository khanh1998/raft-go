package common

import "errors"

var (
	// state machine
	ErrorSessionExpired = errors.New("session expired: no record of session can be found")
)
