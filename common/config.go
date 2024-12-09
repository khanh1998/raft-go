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

// cluster_time_commit_max_duration: 30s
// http_client_request_max_timeout: 60m
// log_extensions:
//   enable: etcd # or 'classic'
//   classic:
//     client_session_duration: 2m
//   etcd:
//     state_machine_history_capacity: 1000
//     state_machine_btree_degree: 32
//     http_client_max_wait_timeout: 60m

type LogExtension string

const (
	LogExtensionClassic LogExtension = "classic"
	LogExtensionEtcd    LogExtension = "etcd"
)

type ConfigLogExtensions struct {
	Enable  LogExtension `mapstructure:"enable"` // `etcd`
	Classic struct {
		ClientSessionDuration time.Duration `mapstructure:"client_session_duration"`
	} `mapstructure:"classic"`
	Etcd struct {
		StateMachineHistoryCapacity int           `mapstructure:"state_machine_history_capacity"`
		StateMachineBTreeDegree     int           `mapstructure:"state_machine_btree_degree"`
		MaxWaitTimeout              time.Duration `mapstructure:"http_client_max_wait_timeout"`
	} `mapstructure:"etcd"`
}

type RaftCoreConfig struct {
	Cluster                      ClusterConfig       `mapstructure:"cluster"`
	MinElectionTimeout           time.Duration       `mapstructure:"min_election_timeout" validate:"required,gt=0"`
	MaxElectionTimeout           time.Duration       `mapstructure:"max_election_timeout" validate:"required,gt=0"`
	MinHeartbeatTimeout          time.Duration       `mapstructure:"min_heartbeat_timeout" validate:"required,gt=0"`
	MaxHeartbeatTimeout          time.Duration       `mapstructure:"max_heartbeat_timeout" validate:"required,gt=0"`
	DataFolder                   string              `mapstructure:"data_folder" default:"data/" validate:"required"`
	WalSizeLimit                 int64               `mapstructure:"wal_size_limit"`
	LogLengthLimit               int                 `mapstructure:"log_length_limit"`
	Observability                ObservabilityConfig `mapstructure:"observability"`
	RpcDialTimeout               time.Duration       `mapstructure:"rpc_dial_timeout"`
	RpcRequestTimeout            time.Duration       `mapstructure:"rpc_request_timeout"`
	RpcReconnectDuration         time.Duration       `mapstructure:"rpc_reconnect_duration"`
	SnapshotChunkSize            int                 `mapstructure:"snapshot_chunk_size"`
	ClusterTimeCommitMaxDuration time.Duration       `mapstructure:"cluster_time_commit_max_duration"`
	HttpClientRequestMaxTimeout  time.Duration       `mapstructure:"http_client_request_max_timeout"`
}

type Config struct {
	Cluster                      ClusterConfig       `mapstructure:"cluster"`
	MinElectionTimeout           time.Duration       `mapstructure:"min_election_timeout" validate:"required,gt=0"`
	MaxElectionTimeout           time.Duration       `mapstructure:"max_election_timeout" validate:"required,gt=0"`
	MinHeartbeatTimeout          time.Duration       `mapstructure:"min_heartbeat_timeout" validate:"required,gt=0"`
	MaxHeartbeatTimeout          time.Duration       `mapstructure:"max_heartbeat_timeout" validate:"required,gt=0"`
	DataFolder                   string              `mapstructure:"data_folder" default:"data/" validate:"required"`
	WalSizeLimit                 int64               `mapstructure:"wal_size_limit"`
	LogLengthLimit               int                 `mapstructure:"log_length_limit"`
	Observability                ObservabilityConfig `mapstructure:"observability"`
	RpcDialTimeout               time.Duration       `mapstructure:"rpc_dial_timeout"`
	RpcRequestTimeout            time.Duration       `mapstructure:"rpc_request_timeout"`
	RpcReconnectDuration         time.Duration       `mapstructure:"rpc_reconnect_duration"`
	SnapshotChunkSize            int                 `mapstructure:"snapshot_chunk_size"`
	ClusterTimeCommitMaxDuration time.Duration       `mapstructure:"cluster_time_commit_max_duration"`
	HttpClientRequestMaxTimeout  time.Duration       `mapstructure:"http_client_request_max_timeout"`
	LogExtensions                ConfigLogExtensions `mapstructure:"log_extensions"`
}

// appModeValidator is a custom validator function to check if the mode is valid
func clusterModeValidator(fl validator.FieldLevel) bool {
	mode := fl.Field().String()
	if mode == string(Static) || mode == string(Dynamic) {
		return true
	}
	return false
}
