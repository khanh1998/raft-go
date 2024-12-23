package common

import (
	gc "khanh/raft-go/common"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	RaftCore      gc.RaftCoreConfig      `mapstructure:"raft_core"`
	Observability gc.ObservabilityConfig `mapstructure:"observability"`
	Extension     struct {
		StateMachineHistoryCapacity int           `mapstructure:"state_machine_history_capacity"`
		StateMachineBTreeDegree     int           `mapstructure:"state_machine_btree_degree"`
		MaxWaitTimeout              time.Duration `mapstructure:"http_client_max_wait_timeout"`
	} `mapstructure:"extension"`
	NetworkSimulation gc.NetworkSimulationConfig `mapstructure:"network_simulation"`
}

func ReadConfigFromFile(filePath *string) (*Config, error) {
	v := viper.New()

	if filePath != nil {
		v.SetConfigFile(*filePath)
	} else {
		v.SetConfigFile("config.yml")
	}
	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	// Unmarshal config into struct
	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, err
	}

	config.RaftCore.StringToTime(v)
	config.NetworkSimulation.StringToTime(v)

	return &config, nil
}
