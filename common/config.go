package common

import (
	"time"

	"github.com/go-playground/validator/v10"
)

type ClusterMode string

var (
	Static  ClusterMode = "static"
	Dynamic ClusterMode = "dynamic"
)

type ClusterServerConfig struct {
	ID       int    `mapstructure:"id" validate:"required,gt=0"`
	HttpPort int    `mapstructure:"http_port" validate:"required,hostname_port"`
	RpcPort  int    `mapstructure:"rpc_port" validate:"required,hostname_port"`
	Host     string `mapstructure:"host" validate:"required,hostname"`
}

type ClusterConfig struct {
	Mode    ClusterMode           `mapstructure:"mode" validate:"required,clustermode"`
	Servers []ClusterServerConfig `mapstructure:"servers"`
}

type ObservabilityConfig struct {
	Disabled      bool   `mapstructure:"disabled"`
	TraceEndpoint string `mapstructure:"trace_endpoint"`
	LogEndpoint   string `mapstructure:"log_endpoint"`
	LokiPushURL   string `mapstructure:"loki_push_url"`
}

type Config struct {
	Cluster               ClusterConfig       `mapstructure:"cluster"`
	MinElectionTimeoutMs  int64               `mapstructure:"min_election_timeout_ms" validate:"required,gt=0"`
	MaxElectionTimeoutMs  int64               `mapstructure:"max_election_timeout_ms" validate:"required,gt=0"`
	MinHeartbeatTimeoutMs int64               `mapstructure:"min_heartbeat_timeout_ms" validate:"required,gt=0"`
	MaxHeartbeatTimeoutMs int64               `mapstructure:"max_heartbeat_timeout_ms" validate:"required,gt=0"`
	DataFolder            string              `mapstructure:"data_folder" default:"data/" validate:"required"`
	StateMachineSnapshot  bool                `mapstructure:"state_machine_snapshot"`
	Observability         ObservabilityConfig `mapstructure:"observability"`
	ClientSessionDuration time.Duration       `mapstructure:"client_session_duration"`
}

// appModeValidator is a custom validator function to check if the mode is valid
func clusterModeValidator(fl validator.FieldLevel) bool {
	mode := fl.Field().String()
	if mode == string(Static) || mode == string(Dynamic) {
		return true
	}
	return false
}
