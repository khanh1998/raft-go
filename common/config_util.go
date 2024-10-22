package common

import (
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

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
	config.ClientSessionDuration, err = time.ParseDuration(viper.GetString("client_session_duration"))
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

	validate := validator.New()
	validate.RegisterValidation("clustermode", clusterModeValidator)

	if err := validate.Struct(config); err != nil {
		return nil, err
	}

	return &config, nil
}
