package common

import (
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

type ConfigRawSource interface {
	GetString(key string) string
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

func ReadConfigFromFile(filePath *string) (*Config, error) {
	if filePath != nil {
		viper.SetConfigFile(*filePath)
	} else {
		viper.SetConfigFile("config.yml")
	}
	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	// Unmarshal config into struct
	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, err
	}

	var err error
	config.LogExtensions.Classic.ClientSessionDuration, err =
		time.ParseDuration(viper.GetString("log_extensions.classic.client_session_duration"))
	if err != nil {
		return nil, err
	}

	config.LogExtensions.Etcd.MaxWaitTimeout, err =
		time.ParseDuration(viper.GetString("log_extensions.etcd.http_client_max_wait_timeout"))
	if err != nil {
		return nil, err
	}

	config.RpcDialTimeout, err = time.ParseDuration(viper.GetString("rpc_dial_timeout"))
	if err != nil {
		return nil, err
	}

	config.RpcRequestTimeout, err = time.ParseDuration(viper.GetString("rpc_request_timeout"))
	if err != nil {
		return nil, err
	}

	config.RpcReconnectDuration, err = time.ParseDuration(viper.GetString("rpc_reconnect_duration"))
	if err != nil {
		return nil, err
	}

	config.MaxElectionTimeout, err = time.ParseDuration(viper.GetString("max_election_timeout"))
	if err != nil {
		return nil, err
	}

	config.MinElectionTimeout, err = time.ParseDuration(viper.GetString("min_election_timeout"))
	if err != nil {
		return nil, err
	}

	config.MaxHeartbeatTimeout, err = time.ParseDuration(viper.GetString("max_heartbeat_timeout"))
	if err != nil {
		return nil, err
	}

	config.MinHeartbeatTimeout, err = time.ParseDuration(viper.GetString("min_heartbeat_timeout"))
	if err != nil {
		return nil, err
	}

	validate := validator.New()
	validate.RegisterValidation("clustermode", clusterModeValidator)

	if err := validate.Struct(config); err != nil {
		return nil, err
	}

	return &config, nil
}
