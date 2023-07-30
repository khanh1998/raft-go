package http_proxy

import (
	"errors"
	"khanh/raft-go/common"
	"net/http"

	"github.com/gin-gonic/gin"
)

type RaftBrain interface {
	ClientRequest(input *common.ClientQueryInput, output *common.ClientQueryOutput) (err error)
	RegisterClient(input *common.RegisterClientInput, output *common.RegisterClientOutput) (err error)
	ClientQuery(input *common.ClientQueryInput, output *common.ClientQueryOutput) (err error)
}

type HttpProxy struct {
	brain RaftBrain
	host  string
}

type NewHttpProxyParams struct {
	URL string
}

func NewHttpProxy(params NewHttpProxyParams) *HttpProxy {
	h := HttpProxy{host: params.URL}

	return &h
}

func (h *HttpProxy) SetBrain(brain RaftBrain) {
	h.brain = brain
}

func (h HttpProxy) Start() {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.POST("/data", func(c *gin.Context) {
		var request common.ClientRequestInput
		if err := c.BindJSON(&request); err != nil {
			return
		}

		// TODO: 	add option so that client can send a synchronize request
		// TODO:	 	solution when client connected to a follower
		// err := h.brain.ServeClientRequest(request)
		err := errors.New("")
		if err != nil {
			c.IndentedJSON(http.StatusInternalServerError, err)
		} else {
			c.IndentedJSON(http.StatusOK, "request accepted")
		}
	})

	go func() {
		r.Run(h.host)
	}()
}
