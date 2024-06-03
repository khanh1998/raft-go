package integration_testing

import (
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/http_proxy"
	"khanh/raft-go/logic"
	"khanh/raft-go/node"
	"khanh/raft-go/persistance"
	"khanh/raft-go/rpc_proxy"
	"log"
	"os"
	"time"

	"github.com/rs/zerolog"
)

func NewDynamicCluster(filePath string) *Cluster {
	c := Cluster{}
	c.initDynamic(filePath)

	return &c
}

func (c *Cluster) createNewNode(id int, rpcUrl, httpUrl string) error {
	catchingUp := id > 1
	param := node.NewNodeParams{
		ID: id,
		Brain: logic.NewRaftBrainParams{
			ID:                  id,
			Mode:                common.Dynamic,
			CachingUp:           catchingUp,
			DataFileName:        fmt.Sprintf("test.log.%d.dat", id),
			HeartBeatTimeOutMin: c.config.MinHeartbeatTimeoutMs,
			HeartBeatTimeOutMax: c.config.MaxHeartbeatTimeoutMs,
			ElectionTimeOutMin:  c.config.MinElectionTimeoutMs,
			ElectionTimeOutMax:  c.config.MaxElectionTimeoutMs,
			Log:                 c.log,
			Members: func() []common.ClusterMember {
				if id == 1 {
					return []common.ClusterMember{{ID: id, RpcUrl: rpcUrl, HttpUrl: httpUrl}}
				}
				return []common.ClusterMember{}
			}(),
			// DB:                persistance.NewPersistence(fmt.Sprintf("test.log.%d.dat", id+i)),
			DB:           persistance.NewPersistenceMock(),
			StateMachine: common.NewKeyValueStateMachine(),
		},
		RPCProxy: rpc_proxy.NewRPCImplParams{
			HostURL: rpcUrl,
			Log:     c.log,
			HostID:  id,
		},
		HTTPProxy: http_proxy.NewHttpProxyParams{
			URL: httpUrl,
		},
	}

	n := node.NewNode(param)
	n.Start(true, catchingUp)

	c.createNodeParams[id] = param
	c.Nodes[id] = n
	c.HttpAgent.serverInfos = append(c.HttpAgent.serverInfos, HttpServerConnectionInfo{
		Id:  id,
		Url: httpUrl,
	})
	c.HttpAgent.serverUrls[id] = httpUrl
	return c.RpcAgent.ConnectToPeer(id, rpcUrl, 5, 1*time.Second)
}

func (c *Cluster) initDynamic(filePath string) {
	config, err := common.ReadConfigFromFile(&filePath)
	if err != nil {
		log.Panic(err)
	}

	c.config = config
	c.MaxElectionTimeout = time.Duration(config.MaxElectionTimeoutMs * 1000 * 1000)
	c.MaxHeartbeatTimeout = time.Duration(config.MaxHeartbeatTimeoutMs * 1000 * 1000)

	zerolog.TimeFieldFormat = time.RFC3339Nano
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339Nano}

	log := zerolog.New(output).With().Timestamp().Logger()
	c.log = &log

	c.Nodes = make(map[int]*node.Node)
	c.createNodeParams = make(map[int]node.NewNodeParams)

	id, rpcUrl, httpUrl := 1, "localhost:1234", "localhost:8080"

	// we will use these rpc and http agent to connect to the cluster
	rpcAgent, err := NewRPCImpl(NewRPCImplParams{
		Peers: []common.ClusterMember{{ID: id, HttpUrl: httpUrl, RpcUrl: rpcUrl}},
		Log:   &log,
	})
	if err != nil {
		panic(err)
	}

	httpServerUrls := []HttpServerConnectionInfo{
		{
			Id:  id,
			Url: httpUrl,
		},
	}

	httpAgent := NewHttpAgent(HttpAgentArgs{
		serverUrls: httpServerUrls,
		Log:        &log,
	})

	c.RpcAgent = *rpcAgent
	c.HttpAgent = *httpAgent

	// create first node of cluster
	err = c.createNewNode(id, rpcUrl, httpUrl)
	if err != nil {
		panic(err)
	}

}
