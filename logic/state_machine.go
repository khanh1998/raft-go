package logic

import "errors"

type StateMachine struct {
	data map[string]int64
}

func NewStateMachine() StateMachine {
	return StateMachine{data: make(map[string]int64)}
}

func (s StateMachine) Get(key string) (exist bool, val int64) {
	if val, exist = s.data[key]; exist {
		return
	}

	return false, 0
}

func (s *StateMachine) Put(entry Entry) {
	if val, ok := s.data[entry.Key]; ok {
		newVal := val
		switch entry.Opcode {
		case Minus:
			newVal -= entry.Value
		case Plus:
			newVal += entry.Value
		case Multiply:
			newVal *= entry.Value
		case Divide:
			newVal /= entry.Value
		case Overwrite:
			newVal = entry.Value
		}

		s.data[entry.Key] = newVal
	} else {
		s.data[entry.Key] = entry.Value
	}
}

type Operator string

var ErrInvalidOperator = errors.New("operator is invalid")

const (
	Minus     Operator = "minus"
	Plus      Operator = "plus"
	Multiply  Operator = "mul"
	Divide    Operator = "div"
	Overwrite Operator = "overwrite"
)

func (o Operator) Valid() error {
	switch o {
	case Minus, Plus, Multiply, Divide, Overwrite:
		return nil
	default:
		return ErrInvalidOperator
	}
}
