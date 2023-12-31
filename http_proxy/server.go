package http_proxy

import (
	"context"
	"errors"
	"khanh/raft-go/common"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

type RaftBrain interface {
	ClientRequest(input *common.ClientRequestInput, output *common.ClientRequestOutput) (err error)
	RegisterClient(input *common.RegisterClientInput, output *common.RegisterClientOutput) (err error)
	ClientQuery(input *common.ClientQueryInput, output *common.ClientQueryOutput) (err error)
}

type HttpProxy struct {
	brain     RaftBrain
	host      string
	Stop      chan struct{}
	Accessile bool
}

type NewHttpProxyParams struct {
	URL string
}

func NewHttpProxy(params NewHttpProxyParams) *HttpProxy {
	h := HttpProxy{host: params.URL, Stop: make(chan struct{}), Accessile: true}

	return &h
}

func (h *HttpProxy) SetBrain(brain RaftBrain) {
	h.brain = brain
}

func (h HttpProxy) clientQuery(r *gin.Engine) {
	r.POST("/query", func(c *gin.Context) {
		if !h.Accessile {
			c.Status(http.StatusRequestTimeout)

			return
		}

		var request common.ClientQueryInput
		if err := c.BindJSON(&request); err != nil {
			return
		}

		var response common.ClientQueryOutput
		err := h.brain.ClientQuery(&request, &response)
		if err != nil {
			c.IndentedJSON(http.StatusInternalServerError, response)
		} else {
			c.IndentedJSON(http.StatusOK, response)
		}
	})
}

func (h HttpProxy) registerClient(r *gin.Engine) {
	r.POST("/register", func(c *gin.Context) {
		if !h.Accessile {
			c.Status(http.StatusRequestTimeout)

			return
		}
		var request common.RegisterClientInput
		if err := c.BindJSON(&request); err != nil {
			return
		}

		var response common.RegisterClientOutput

		err := h.brain.RegisterClient(&request, &response)
		if err != nil {
			c.IndentedJSON(http.StatusInternalServerError, response)
		} else {
			c.IndentedJSON(http.StatusOK, response)
		}
	})
}

func (h HttpProxy) clientRequest(r *gin.Engine) {
	r.POST("/command", func(c *gin.Context) {
		if !h.Accessile {
			c.Status(http.StatusRequestTimeout)

			return
		}
		var request common.ClientRequestInput
		if err := c.BindJSON(&request); err != nil {
			return
		}

		if request.ClientID <= 0 || request.SequenceNum <= 0 || request.Command == nil {
			c.Error(errors.New("invalid data"))
			return
		}

		var response common.ClientRequestOutput
		err := h.brain.ClientRequest(&request, &response)
		if err != nil {
			c.IndentedJSON(http.StatusInternalServerError, response)
		} else {
			c.IndentedJSON(http.StatusOK, response)
		}
	})
}

func (h HttpProxy) Start() {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	h.clientRequest(r)
	h.registerClient(r)
	h.clientQuery(r)

	httpServer := &http.Server{
		Addr:    h.host,
		Handler: r,
	}

	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			log.Err(err).Msg("HttpProxy Start")
		}
	}()

	go func() {
		<-h.Stop
		httpServer.Shutdown(context.Background())
	}()
}
