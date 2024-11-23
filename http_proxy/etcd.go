package http_proxy

import (
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

func (h *HttpProxy) Get(c *gin.Context) {
	ctx, span := tracer.Start(c.Request.Context(), "get")
	defer span.End()
	_ = ctx
}

func (h *HttpProxy) NewEtcdInterface(r *gin.Engine) {
	r.GET("/keys/*key", func(ctx *gin.Context) {
		// input := common.EtcdCommand{}
		// output := common.ClientQueryOutput{}
		// h.brain.ClientQuery(ctx, input, &output)
	})
}
