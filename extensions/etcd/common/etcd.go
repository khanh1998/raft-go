package common

import "encoding/json"

const (
	AddServer    = "add-server"
	RemoveServer = "remove-server"
)

type EtcdResultPromise chan EtcdResultRes

func CreateError(key string, err error) error {
	return EtcdResultErr{
		Cause:     key,
		ErrorCode: 400,
		Message:   "invalid value: " + err.Error(),
	}
}

type EtcdResultErr struct {
	Cause     string `json:"cause"`
	ErrorCode int    `json:"errorCode"`
	Index     int    `json:"index"`
	Message   string `json:"message"`
}

func (e EtcdResultErr) Error() string {
	return e.Message
}

type EtcdResult struct {
	Data    EtcdResultRes
	Promise EtcdResultPromise
}

type EtcdResultRes struct {
	ChangeIndex int        `json:"-"`
	Action      string     `json:"action"`
	Node        KeyValue   `json:"node,omitempty"`
	Nodes       []KeyValue `json:"nodes,omitempty"` // to get prefix
	PrevNode    KeyValue   `json:"prevNode,omitempty"`
	PrevNodes   []KeyValue `json:"prevNodes,omitempty"` // to delete prefix
}

type KeyValue struct {
	Key            string `json:"key,omitempty"`
	Value          string `json:"value,omitempty"`
	ModifiedIndex  int    `json:"modifiedIndex,omitempty"`
	CreatedIndex   int    `json:"createdIndex,omitempty"`
	ExpirationTime uint64 `json:"expirationTime,omitempty"` // cluster time
}

type KeyExpire struct {
	Key            string
	ExpirationTime uint64
}

func (k KeyValue) Expired(curr uint64) bool {
	if k.ExpirationTime == 0 {
		return false
	} else {
		return k.ExpirationTime <= curr
	}
}

func (k KeyValue) ToString() string {
	data, _ := json.Marshal(k)
	return string(data)
}

func (k *KeyValue) FromString(data string) error {
	dataByte := []byte(data)
	if err := json.Unmarshal(dataByte, k); err != nil {
		return err
	}
	return nil
}
