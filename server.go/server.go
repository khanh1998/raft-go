package server

import (
	"khanh/raft-go/logic"
	"net/http"
	"net/rpc"

	"github.com/rs/zerolog/log"
)

type RPCServer interface{}

type RPCServerImpl struct {
	Raft  logic.Node
	Peers []*rpc.Client
}

type userHandler struct{}

func (h *userHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// all users request are going to be routed here
}

func route() {
	handler := userHandler{}
	mux := http.NewServeMux()
	mux.Handle("/data/", &handler)
	http.ListenAndServe(":8080", mux)
}

func NewRPCServer(peerUrls []string) RPCServer {
	peers := []*rpc.Client{}
	for _, url := range peerUrls {
		client, err := rpc.DialHTTP("tcp", url)
		if err != nil {
			log.Err(err).Msg("Client connection error: ")
		}

		peers = append(peers, client)
	}

	return RPCServerImpl{}
}

func (r RPCServerImpl) broadcast(method string, input any, output *any) {
	for _, client := range r.Peers {
		err := client.Call(method, input, output)
		if err != nil {
			log.Err(err).Msg("Client invocation error: ")
		}
	}
}

func (r *RPCServerImpl) BroadcastAppendEntries() {

}

func (r *RPCServerImpl) BroadcastRequestVote() {

}
