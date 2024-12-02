package common

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// mimic etcd 2.3 api
type EtcdLog struct {
	Term    int         `json:"term"`
	Command EtcdCommand `json:"command"`
	Time    uint64      `json:"time"`
}

// nil value for an field means client didn't provide value for it, even default value.
type EtcdCommand struct {
	Internal bool   `json:"internal,omitempty"` // is it a internal command?
	Action   string `json:"action,omitempty"`   // http method: get, put, delete
	Key      string `json:"key,omitempty"`

	Value     *string `json:"value,omitempty"`     // nil, empty or not-empty
	Ttl       uint64  `json:"ttl,omitempty"`       // nanoseconds // positive or zero(nil)
	PrevExist *bool   `json:"prevExist,omitempty"` // nil, true or false
	PrevValue *string `json:"prevValue,omitempty"` // nil, empty or not-empty
	PrevIndex int     `json:"prevIndex,omitempty"` // positive or zero(nil)

	Refresh   bool `json:"refresh,omitempty"`   // true or false(nil)
	Wait      bool `json:"wait,omitempty"`      // true or false(nil)
	WaitIndex int  `json:"waitIndex,omitempty"` // not zero or zero(nil)
	Prefix    bool `json:"prefix,omitempty"`    // true or false(nil)
}

func (e EtcdCommand) Causes() string {
	data := []string{}
	if e.PrevExist != nil {
		data = append(data, fmt.Sprintf("prevExist=%v", *e.PrevExist))
	}
	if e.PrevValue != nil {
		data = append(data, fmt.Sprintf("prevValue=%v", *e.PrevValue))
	}
	if e.PrevIndex > 0 {
		data = append(data, fmt.Sprintf("prevIndex=%v", e.PrevIndex))
	}
	return fmt.Sprintf("[%v]", strings.Join(data, ","))
}

func (e EtcdLog) Serialize() []byte {
	data, _ := json.Marshal(e)
	return data
}
func (e EtcdLog) ToString() string {
	data, _ := json.Marshal(e)
	return string(data)
}
func (e EtcdLog) GetTerm() int {
	return e.Term
}
func (e EtcdLog) GetTime() uint64 {
	return e.Time
}

func (e EtcdLog) DecomposeChangeSeverCommand() (addition bool, serverId int, httpUrl string, rpcUrl string, err error) {
	return
}

type EtcdLogFactory struct {
	NewSnapshot func() Snapshot
}

func (c EtcdLogFactory) EmptySnapshot() Snapshot {
	return c.NewSnapshot()
}

func (c EtcdLogFactory) Deserialize(data []byte) (Log, error) {
	log := EtcdLog{}
	err := json.Unmarshal(data, &log)
	if err != nil {
		return EtcdLog{}, err
	}
	return log, nil
}

func (c EtcdLogFactory) FromString(data string) (Log, error) {
	byteArr := []byte(data)
	log := EtcdLog{}
	err := json.Unmarshal(byteArr, &log)
	if err != nil {
		return EtcdLog{}, err
	}
	return log, nil
}

// attach term and time to an exist log.
func (c EtcdLogFactory) AttachTermAndTime(logI Log, term int, time uint64) (Log, error) {
	log, ok := logI.(EtcdLog)
	if !ok {
		return EtcdLog{}, errors.New("invalid log type")
	}

	log.Term = term
	log.Time = time

	return log, nil
}

func (c EtcdLogFactory) Empty() Log {
	return EtcdLog{}
}

func (c EtcdLogFactory) NoOperation(term int, time uint64) Log {
	return EtcdLog{Term: term, Time: time, Command: EtcdCommand{Internal: true, Action: NoOperation}}
}

func (c EtcdLogFactory) AddNewNode(term int, time uint64, nodeId int, httpUrl string, rpcUrl string) Log {
	return EtcdLog{
		Term:    term,
		Time:    time,
		Command: EtcdCommand{},
	}
}

func (c EtcdLogFactory) RemoveNode(term int, time uint64, nodeId int, httpUrl string, rpcUrl string) Log {
	return EtcdLog{
		Term:    term,
		Time:    time,
		Command: EtcdCommand{},
	}
}

func (c EtcdLogFactory) CreateTimeCommit(term int, nanosecond uint64) Log {
	return EtcdLog{Term: term, Time: nanosecond, Command: EtcdCommand{Internal: true, Action: TimeCommit}}
}
