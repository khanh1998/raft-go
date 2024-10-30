package common

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type AddServerInput struct {
	ID               int
	NewServerHttpUrl string
	NewServerRpcUrl  string
}

type AddServerOutput struct {
	Status     ClientRequestStatus `json:"status"`
	LeaderHint string              `json:"leader_hint"`
	Response   string              `json:"response"`
}

type RemoveServerInput struct {
	ID               int
	NewServerHttpUrl string
	NewServerRpcUrl  string
}

type RemoveServerOutput struct {
	Status     ClientRequestStatus `json:"status"`
	LeaderHint string              `json:"leader_hint"`
	Response   string              `json:"response"`
}

func ComposeAddServerCommand(serverId int, httpUrl string, rpcUrl string) string {
	return fmt.Sprintf("addServer %d %s %s", serverId, httpUrl, rpcUrl)
}

func ComposeRemoveServerCommand(serverId int, httpUrl string, rpcUrl string) string {
	return fmt.Sprintf("removeServer %d %s %s", serverId, httpUrl, rpcUrl)
}

func DecomposeChangeSeverCommand(command string) (addition bool, serverId int, httpUrl string, rpcUrl string, err error) {
	serverId, httpUrl, rpcUrl, err = DecomposeAddServerCommand(command)
	if err != nil {
		serverId, httpUrl, rpcUrl, err = DecomposeRemoveServerCommand(command)
	} else {
		addition = true
	}

	return
}

func DecomposeAddServerCommand(command string) (serverId int, httpUrl string, rpcUrl string, err error) {
	tokens := strings.Split(command, " ")
	if len(tokens) != 4 {
		err = errors.New("not enough args for addServer command")
		return
	}

	if tokens[0] == "addServer" {
		serverId, err = strconv.Atoi(tokens[1])
		if err != nil {
			return
		}

		httpUrl, rpcUrl = trimAndLower(tokens[2]), trimAndLower(tokens[3])
	} else {
		err = errors.New("not addServer command")
	}

	return
}

func DecomposeRemoveServerCommand(command string) (serverId int, httpUrl string, rpcUrl string, err error) {
	tokens := strings.Split(command, " ")
	if len(tokens) != 4 {
		err = errors.New("not enough args for removeServer command")
		return
	}

	if tokens[0] == "removeServer" {
		serverId, err = strconv.Atoi(tokens[1])
		if err != nil {
			return
		}

		httpUrl, rpcUrl = trimAndLower(tokens[2]), trimAndLower(tokens[3])
	} else {
		err = errors.New("not removeServer command")
	}

	return
}

type ClusterMemberChange struct {
	ClusterMember
	Add   bool
	Reset bool
}

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
