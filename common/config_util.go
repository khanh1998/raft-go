package common

import (
	"time"
)

type ConfigRawSource interface {
	GetString(key string) string
}

func (config *NetworkSimulationConfig) StringToTime(s ConfigRawSource) error {
	var err error
	config.MinDelay, err = time.ParseDuration(s.GetString("min_delay"))
	if err != nil {
		return err
	}

	config.MinDelay, err = time.ParseDuration(s.GetString("max_delay"))
	if err != nil {
		return err
	}

	return nil
}

// convert raw string in config file to time in Go
func (config *RaftCoreConfig) StringToTime(s ConfigRawSource) error {
	var err error
	config.RpcDialTimeout, err = time.ParseDuration(s.GetString("rpc_dial_timeout"))
	if err != nil {
		return err
	}

	config.RpcRequestTimeout, err = time.ParseDuration(s.GetString("rpc_request_timeout"))
	if err != nil {
		return err
	}

	config.RpcReconnectDuration, err = time.ParseDuration(s.GetString("rpc_reconnect_duration"))
	if err != nil {
		return err
	}

	config.MaxElectionTimeout, err = time.ParseDuration(s.GetString("max_election_timeout"))
	if err != nil {
		return err
	}

	config.MinElectionTimeout, err = time.ParseDuration(s.GetString("min_election_timeout"))
	if err != nil {
		return err
	}

	config.MaxHeartbeatTimeout, err = time.ParseDuration(s.GetString("max_heartbeat_timeout"))
	if err != nil {
		return err
	}

	config.MinHeartbeatTimeout, err = time.ParseDuration(s.GetString("min_heartbeat_timeout"))
	if err != nil {
		return err
	}

	return nil
}
