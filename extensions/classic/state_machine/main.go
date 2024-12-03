package state_machine

import (
	"context"
	"errors"
	"khanh/raft-go/common"
)

type RaftPersistenceState interface {
	SaveSnapshot(ctx context.Context, snapshot common.Snapshot) (err error)
	ReadLatestSnapshot(ctx context.Context) (snap common.Snapshot, err error)
}

var (
	ErrKeyDoesNotExist            = errors.New("key does not exist")
	ErrKeyMustBeString            = errors.New("key must be string")
	ErrValueMustBeString          = errors.New("value must be string")
	ErrCommandIsEmpty             = errors.New("command is empty")
	ErrUnsupportedCommand         = errors.New("command is unsupported")
	ErrNotEnoughParameters        = errors.New("not enough parameters")
	ErrorSequenceNumProcessed     = errors.New("sequence number already processed")
	ErrCommandWasSnapshot         = errors.New("the command is included in the snapshot")
	ErrDataFileNameIsEmpty        = errors.New("data file name is empty")
	ErrKeyIsLocked                = errors.New("key is locked by another client")
	ErrInvalidParameters          = errors.New("invalid parameters")
	ErrInputLogTypeIsNotSupported = errors.New("input log type is not supported by state machine")
	ErrorSessionExpired           = errors.New("user session expired")
)
