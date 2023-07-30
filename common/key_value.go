package common

import (
	"errors"
	"strings"
)

var (
	ErrKeyDoesNotExist     = errors.New("key does not exist")
	ErrKeyMustBeString     = errors.New("key must be string")
	ErrValueMustBeString   = errors.New("value must be string")
	ErrCommandIsEmpty      = errors.New("command is empty")
	ErrUnsupportedCommand  = errors.New("command is unsupported")
	ErrNotEnoughParameters = errors.New("not enough parameters")
)

type KeyValueStateMachine struct {
	data map[string]string
}

func NewKeyValueStateMachine() *KeyValueStateMachine {
	return &KeyValueStateMachine{data: make(map[string]string)}
}

func (k KeyValueStateMachine) Process(commandIn any) (result any, err error) {
	command, ok := commandIn.(string)
	if !ok {
		return "", ErrKeyMustBeString
	}

	if len(command) == 0 {
		return "", ErrCommandIsEmpty
	}

	tokens := strings.Split(command, " ")
	cmd := strings.ToLower(tokens[0])

	switch cmd {
	case "get":
		if len(tokens) < 2 {
			return "", ErrNotEnoughParameters
		}

		key := tokens[1]

		return k.get(key)
	case "set":
		if len(tokens) < 3 {
			return "", ErrNotEnoughParameters
		}

		key := tokens[1]
		value := tokens[2]

		return k.set(key, value)
	}

	return "", ErrUnsupportedCommand
}

func (k KeyValueStateMachine) get(key string) (value string, err error) {
	value, ok := k.data[key]
	if !ok {
		return "", ErrKeyDoesNotExist
	}

	return value, nil
}

func (k *KeyValueStateMachine) set(key string, value string) (string, error) {
	k.data[key] = value

	return value, nil
}

func (k KeyValueStateMachine) GetAll() (data map[any]any, err error) {
	data = map[any]any{}

	for key, value := range k.data {
		data[key] = value
	}

	return data, nil
}
