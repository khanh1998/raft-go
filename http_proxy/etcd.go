package http_proxy

import (
	"khanh/raft-go/observability"

	"github.com/gin-gonic/gin"
)

type EtcdNode struct {
	CreatedIndex  int    `json:"createdIndex"`
	Key           string `json:"key"`
	ModifiedIndex int    `json:"modifiedIndex"`
	Value         string `json:"value"`
}

type GetResponse struct {
	Action string   `json:"action"`
	Node   EtcdNode `json:"node"`
}

func (h *EtcdHttpProxy) SetBrain(brain RaftBrain) {}

func (h *EtcdHttpProxy) Get(c *gin.Context) {
	ctx, span := tracer.Start(c.Request.Context(), "get")
	defer span.End()
	_ = ctx
}

func (h *EtcdHttpProxy) NewEtcdInterface(r *gin.Engine) {
	r.GET("/keys/*key", func(ctx *gin.Context) {
		// input := common.EtcdCommand{}
		// output := common.ClientQueryOutput{}
		// h.brain.ClientQuery(ctx, input, &output)
	})
}

type EtcdHttpProxy struct {
	brain      RaftBrain
	host       string
	stop       chan struct{}
	accessible bool
	logger     observability.Logger
}

type NewEtcdHttpProxyParams struct {
	URL    string
	Logger observability.Logger
}

func NewEtcdHttpProxy(params NewEtcdHttpProxyParams) *ClassicHttpProxy {
	h := ClassicHttpProxy{
		host:       params.URL,
		stop:       make(chan struct{}),
		accessible: true,
		logger:     params.Logger,
	}

	return &h
}
