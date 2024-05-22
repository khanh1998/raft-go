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

	ClientID    int
	SequenceNum int
}

type AddServerOutput struct {
	Status     ClientRequestStatus `json:"status"`
	LeaderHint string              `json:"leader_hint"`
}

type RemoveServerInput struct {
	ID               int
	NewServerHttpUrl string
	NewServerRpcUrl  string

	ClientID    int
	SequenceNum int
}

type RemoveServerOutput struct {
	Status     ClientRequestStatus `json:"status"`
	LeaderHint string              `json:"leader_hint"`
}

func ComposeAddServerCommand(serverId int, httpUrl string, rpcUrl string) string {
	return fmt.Sprintf("addServer %d %s %s", serverId, httpUrl, rpcUrl)
}

func ComposeRemoveServerCommand(serverId int, httpUrl string, rpcUrl string) string {
	return fmt.Sprintf("removeServer %d %s %s", serverId, httpUrl, rpcUrl)
}

func DecomposeAddSeverCommand(command string) (serverId int, httpUrl string, rpcUrl string, err error) {
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

		httpUrl, rpcUrl = tokens[2], tokens[3]
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

		httpUrl, rpcUrl = tokens[2], tokens[3]
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
