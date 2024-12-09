package common

import (
	"context"
	"flag"
	"fmt"
)

func ReadCmdFlagsForStatic() (id int, err error) {
	flag.IntVar(&id, "id", -1, "node ID (required, > 0)")
	// corresponding rpc port and http port will be get from the config file
	flag.Parse()
	if id <= 0 {
		return 0, fmt.Errorf("node's `id` is required, id > 0")
	}
	return id, nil
}

func ReadCmdFlagsForDynamic() (id int, catchingUp bool, httpPort int, rpcPort int, err error) {
	flag.IntVar(&id, "id", -1, "node ID (required, > 0)")
	flag.BoolVar(&catchingUp, "catching-up", false, "start as follower and catch up with leader")
	flag.IntVar(&rpcPort, "rpc-port", -1, "internal RPC port (required, > 0)")
	flag.IntVar(&httpPort, "http-port", -1, "HTTP port for client communication (required, > 0)")
	// if catchingUp is false, the node will be the follower in one-member-cluster. the follower then become the leader after that.
	// if catchingUP is true, the node will receive logs from leader. once the node is cautch up with the leader,
	// it will be add to the cluster as a follower.
	// each cluster can have at most one node start with catching-up = false, this is the first node of the cluster,
	// following nodes have to set catching-up = true.

	flag.Parse()

	if id <= 0 {
		return 0, false, 0, 0, fmt.Errorf("node ID is required (> 0)")
	}
	if rpcPort <= 0 {
		return 0, false, 0, 0, fmt.Errorf("RPC port is required (> 0)")
	}
	if httpPort <= 0 {
		return 0, false, 0, 0, fmt.Errorf("HTTP port is required (> 0)")
	}

	return id, catchingUp, httpPort, rpcPort, nil
}

type NodeIdentifiers struct {
	ID         int
	HttpUrl    string
	RpcUrl     string
	CatchingUp bool
}

func ReadCmdFlags(ctx context.Context, clusterMode ClusterMode, servers []ClusterServerConfig) (n NodeIdentifiers, err error) {
	if clusterMode == Static {
		id, err := ReadCmdFlagsForStatic()
		if err != nil {
			return n, err
		}
		n.ID = id

		found := false
		for _, s := range servers {
			if s.ID == n.ID {
				n.HttpUrl = fmt.Sprintf("%s:%d", s.Host, s.HttpPort)
				n.RpcUrl = fmt.Sprintf("%s:%d", s.Host, s.RpcPort)
				found = true
				break
			}
		}

		if !found {
			return n, fmt.Errorf("server id [%d] didn't exist in cluster server config", id)
		}
	}

	if clusterMode == Dynamic {
		id, catchingUp, httpPort, rpcPort, err := ReadCmdFlagsForDynamic()
		if err != nil {
			return n, err
		}

		n.ID = id
		n.CatchingUp = catchingUp
		n.HttpUrl = fmt.Sprintf("localhost:%d", httpPort)
		n.RpcUrl = fmt.Sprintf(":%d", rpcPort)
	}

	return n, nil
}
