package common

import (
	"fmt"
	"strconv"
	"strings"
)

type ClientEntry struct {
	LastSequenceNum int
	LastResponse    string
	ExpiryTime      uint64
	LockedKeys      map[string]struct{}
}

func (c ClientEntry) ToString(clientId int) string {
	return fmt.Sprintf("%d|%d|%d|%v", clientId, c.LastSequenceNum, c.ExpiryTime, c.LastResponse)
}

func (c *ClientEntry) FromString(str string) (clientId int, err error) {
	tokens := strings.Split(str, "|")
	clientId, err = strconv.Atoi(tokens[0])
	if err != nil {
		return clientId, err
	}

	c.LastSequenceNum, err = strconv.Atoi(tokens[1])
	if err != nil {
		return clientId, err
	}

	c.ExpiryTime, err = strconv.ParseUint(tokens[2], 10, 64)
	if err != nil {
		return clientId, err
	}

	c.LastResponse = tokens[3]

	return clientId, nil
}
