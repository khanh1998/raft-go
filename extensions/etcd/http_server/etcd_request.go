package http_server

import (
	"errors"
	"khanh/raft-go/extensions/etcd/common"
	"net/http"
	"strings"
	"time"
)

type EtcdCommandRequest struct {
	Action string `json:"action,omitempty"` // http method: get, put, delete
	Key    string `json:"key,omitempty"`

	Value     *string        `json:"value,omitempty"` // nil, empty or not-empty
	Ttl       *time.Duration `json:"ttl,omitempty"`   // nanoseconds // nil, 0, positive
	PrevExist *bool          `json:"prevExist,omitempty"`
	PrevValue *string        `json:"prevValue,omitempty"`
	PrevIndex *int           `json:"prevIndex,omitempty"`

	Refresh   *bool `json:"refresh,omitempty"`   // true or false
	Wait      *bool `json:"wait,omitempty"`      // true or false
	WaitIndex *int  `json:"waitIndex,omitempty"` // zero or not zero
	Prefix    *bool `json:"prefix,omitempty"`    // true or false
}

func (e EtcdCommandRequest) ToCommandGet() common.EtcdCommand {
	cmd := common.EtcdCommand{
		Action: e.Action,
		Key:    e.Key,
	}

	if e.Wait != nil {
		cmd.Wait = *e.Wait
	}

	if e.WaitIndex != nil {
		cmd.WaitIndex = *e.WaitIndex
	}

	if e.Prefix != nil {
		cmd.Prefix = *e.Prefix
	}

	return cmd
}

func (e EtcdCommandRequest) ToCommandDelete() common.EtcdCommand {
	cmd := common.EtcdCommand{
		Action:    e.Action,
		Key:       e.Key,
		PrevExist: e.PrevExist,
		PrevValue: e.PrevValue,
	}

	if e.PrevIndex != nil {
		cmd.PrevIndex = *e.PrevIndex
	}

	if e.Prefix != nil {
		cmd.Prefix = *e.Prefix
	}

	return cmd
}

func (e EtcdCommandRequest) ToCommandPut() common.EtcdCommand {
	cmd := common.EtcdCommand{
		Action:    e.Action,
		Key:       e.Key,
		Value:     e.Value,
		PrevExist: e.PrevExist,
		PrevValue: e.PrevValue,
	}

	if e.Ttl != nil {
		cmd.Ttl = uint64(*e.Ttl)
	}

	if e.PrevIndex != nil {
		cmd.PrevIndex = *e.PrevIndex
	}

	if e.Refresh != nil {
		cmd.Refresh = *e.Refresh
	}

	return cmd
}

func (e EtcdCommandRequest) ValidateGet() error {
	if e.Action != strings.ToLower(http.MethodGet) {
		return common.CreateError("action", errors.New("`action` must be `get`"))
	}

	if e.Key == "" {
		return common.CreateError("key", errors.New("`key` can't be empty string"))
	}

	if e.WaitIndex != nil {
		// index 0 is meaningless in Raft
		if *e.WaitIndex <= 0 {
			return common.CreateError("waitIndex", errors.New("`waitIndex` must be larger than 0"))
		}

		if e.Wait == nil || !(*e.Wait) {
			return common.CreateError("wait", errors.New("`waitIndex` must go together with `wait=true`"))
		}
	}

	return nil
}

func (e EtcdCommandRequest) ValidatePut() error {
	if e.Action != strings.ToLower(http.MethodPut) {
		return common.CreateError("action", errors.New("`action` must be `put`"))
	}

	if e.Key == "" {
		return common.CreateError("key", errors.New("`key` can't be empty string"))
	}

	if e.PrevExist != nil && !(*e.PrevExist) {
		if e.PrevValue != nil {
			return common.CreateError("prevValue", errors.New("conflicted preconditions: `prevExist=false` can't go with `prevValue`"))
		}
		if e.PrevIndex != nil {
			return common.CreateError("prevIndex", errors.New("conflicted preconditions: `prevExist=false` can't go with `prevIndex`"))
		}
	}

	// index 0 is meaningless in Raft
	if e.PrevIndex != nil && *e.PrevIndex <= 0 {
		return common.CreateError("prevIndex", errors.New("`prevIndex` must be larger than 0"))
	}

	if e.Refresh != nil && *e.Refresh {
		if e.Value != nil {
			return common.CreateError("value", errors.New("`refresh=true` must not go together with `value`"))
		}
		if e.Ttl == nil {
			return common.CreateError("ttl", errors.New("`refresh=true` must go together with `ttl`"))
		}
	} else {
		if e.Value == nil {
			return common.CreateError("value", errors.New("must provide `value`"))
		}
	}

	if e.Ttl != nil && *e.Ttl <= 0 {
		return common.CreateError("ttl", errors.New("`ttl` must be larger than 0"))
	}

	return nil
}

func (e EtcdCommandRequest) ValidateDelete() error {
	if e.Action != strings.ToLower(http.MethodDelete) {
		return common.CreateError("action", errors.New("`action` must be `delete`"))
	}

	if e.Key == "" {
		return common.CreateError("key", errors.New("`key` can't be empty string"))
	}

	if e.PrevExist != nil && !(*e.PrevExist) {
		if e.PrevValue != nil {
			return common.CreateError("prevValue", errors.New("conflicted preconditions: `prevExist=false` can't go with `prevValue`"))
		}
		if e.PrevIndex != nil {
			return common.CreateError("prevIndex", errors.New("conflicted preconditions: `prevExist=false` can't go with `prevIndex`"))
		}
	}

	// index 0 is meaningless in Raft
	if e.PrevIndex != nil && *e.PrevIndex <= 0 {
		return common.CreateError("prevIndex", errors.New("`prevIndex` must be larger than 0"))
	}

	return nil
}
