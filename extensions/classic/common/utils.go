package common

import (
	"errors"
	"fmt"
	gc "khanh/raft-go/common"
	"strconv"
	"strings"
)

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

		httpUrl, rpcUrl = gc.TrimAndLower(tokens[2]), gc.TrimAndLower(tokens[3])
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

		httpUrl, rpcUrl = gc.TrimAndLower(tokens[2]), gc.TrimAndLower(tokens[3])
	} else {
		err = errors.New("not removeServer command")
	}

	return
}
