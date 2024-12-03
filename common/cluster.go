package common

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type ClusterMember struct {
	ID      int    `mapstructure:"id"`
	RpcUrl  string `mapstructure:"rpc_url"`
	HttpUrl string `mapstructure:"http_url"`
}

func (c ClusterMember) ToString() string {
	return fmt.Sprintf("%d|%s|%s", c.ID, c.HttpUrl, c.RpcUrl)
}

var ErrInvalidClusterMemberString = errors.New("invalid cluster member string")

func (c *ClusterMember) FromString(s string) error {
	tokens := strings.Split(s, "|")
	if len(tokens) != 3 {
		return ErrInvalidClusterMemberString
	}

	id, err := strconv.Atoi(tokens[0])
	if err != nil {
		return errors.Join(ErrInvalidClusterMemberString, err)
	}

	c.ID = id
	c.HttpUrl = tokens[1]
	c.RpcUrl = tokens[2]

	return nil
}
