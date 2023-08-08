package common

import (
	"errors"
	"strings"

	"github.com/rs/zerolog/log"
)

var (
	ErrKeyDoesNotExist        = errors.New("key does not exist")
	ErrKeyMustBeString        = errors.New("key must be string")
	ErrValueMustBeString      = errors.New("value must be string")
	ErrCommandIsEmpty         = errors.New("command is empty")
	ErrUnsupportedCommand     = errors.New("command is unsupported")
	ErrNotEnoughParameters    = errors.New("not enough parameters")
	ErrorSessionExpired       = errors.New("session expired: no record of session can be found")
	ErrorSequenceNumProcessed = errors.New("sequence number already processed")
)

type ClientEntry struct {
	LastSequenceNum int
	LastResponse    any
}

type KeyValueStateMachine struct {
	data  map[string]string
	cache map[int]ClientEntry
}

func NewKeyValueStateMachine() *KeyValueStateMachine {
	return &KeyValueStateMachine{
		data:  make(map[string]string),
		cache: make(map[int]ClientEntry),
	}
}

func (k KeyValueStateMachine) setCache(clientID int, sequenceNum int, response any) *ClientEntry {
	if clientID > 0 && sequenceNum >= 0 {
		data := ClientEntry{
			LastSequenceNum: sequenceNum,
			LastResponse:    response,
		}

		k.cache[clientID] = data

		return &data
	}
	return nil
}

func (k KeyValueStateMachine) Process(clientID int, sequenceNum int, commandIn any, logIndex int) (result any, err error) {
	client, ok := k.cache[clientID]
	if clientID > 0 && !ok {
		return nil, ErrorSessionExpired
	}

	defer func() {
		k.setCache(clientID, sequenceNum, result)

		log.Info().
			Int("client id", clientID).
			Int("sequence num", sequenceNum).
			Interface("command", commandIn).
			Int("log index", logIndex).
			Msg("Process")
		log.Info().
			Interface("data", k.data).
			Interface("cache", k.cache).
			Msg("Process")
	}()

	if sequenceNum > 0 && sequenceNum < client.LastSequenceNum {
		return nil, ErrorSequenceNumProcessed
	}

	if sequenceNum > 0 && sequenceNum == client.LastSequenceNum {
		return client.LastResponse, nil
	}

	command, clientExist := commandIn.(string)
	if !clientExist {
		return nil, ErrKeyMustBeString
	}

	if len(command) == 0 {
		return nil, ErrCommandIsEmpty
	}

	if strings.EqualFold(command, NoOperation) {
		return nil, nil
	}

	tokens := strings.Split(command, " ")
	cmd := strings.ToLower(tokens[0])

	switch cmd {
	case "get":
		if len(tokens) < 2 {
			return nil, ErrNotEnoughParameters
		}

		key := tokens[1]

		return k.get(key)
	case "set":
		if len(tokens) < 3 {
			return nil, ErrNotEnoughParameters
		}

		key := tokens[1]
		value := tokens[2]

		return k.set(key, value)
	case "register":
		clientID = logIndex
		sequenceNum = 0
		result = nil
		k.cache[clientID] = ClientEntry{} // register

		return nil, nil
	}

	return nil, ErrUnsupportedCommand
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
