package http_proxy

import (
	"khanh/raft-go/common"
	"net/http"

	"github.com/gin-gonic/gin"
)

type RaftBrain interface {
	ServeClientRequest(req common.ClientRequest) error
}

type HttpProxy struct {
	brain RaftBrain
}

type NewHttpProxyParams struct {
	URL string
}

func NewHttpProxy(params NewHttpProxyParams) *HttpProxy {
	h := HttpProxy{}
	h.initApi(params.URL)

	return &h
}

func (h *HttpProxy) SetBrain(brain RaftBrain) {
	h.brain = brain
}

func (h HttpProxy) initApi(url string) {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.POST("/data", func(c *gin.Context) {
		var request common.ClientRequest
		if err := c.BindJSON(&request); err != nil {
			return
		}

		err := h.brain.ServeClientRequest(request)
		if err != nil {
			c.IndentedJSON(http.StatusInternalServerError, err)
		} else {
			c.IndentedJSON(http.StatusOK, "request accepted")
		}
	})

	go func() {
		r.Run(url)
	}()
}
