package http_proxy

import (
	"context"
	"errors"
	"khanh/raft-go/common"
	"net/http"
	"regexp"

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

// deprecated
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

// deprecated
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

// deprecated
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

func (h HttpProxy) cli(r *gin.Engine) {
	r.POST("/cli", func(c *gin.Context) {
		if !h.Accessile {
			c.Status(http.StatusRequestTimeout)

			return
		}
		var responseData common.ClientRequestOutput
		var requestData common.ClientRequestInput
		if err := c.BindJSON(&requestData); err != nil {
			return
		}

		errs, cmdType := verifyRequest(requestData)
		if len(errs) > 0 {
			c.IndentedJSON(http.StatusBadRequest, errs)
		}

		var (
			err error
		)

		switch cmdType {
		case CommandTypeGet:
			var response common.ClientQueryOutput
			request := common.ClientQueryInput{
				Query: requestData.Command,
			}
			err = h.brain.ClientQuery(&request, &response)
			responseData = common.ClientRequestOutput{
				Status:     response.Status,
				Response:   response.Response,
				LeaderHint: response.LeaderHint,
			}
		case CommandTypeSet:
			request := common.ClientRequestInput{
				ClientID:    requestData.ClientID,
				SequenceNum: requestData.SequenceNum,
				Command:     requestData.Command,
			}
			var response common.ClientRequestOutput
			err = h.brain.ClientRequest(&request, &response)
			responseData = common.ClientRequestOutput{
				Status:     response.Status,
				Response:   response.Response,
				LeaderHint: response.LeaderHint,
			}
		case CommandTypeRegister:
			var request common.RegisterClientInput
			var response common.RegisterClientOutput
			err = h.brain.RegisterClient(&request, &response)
			responseData = common.ClientRequestOutput{
				Status:     response.Status,
				LeaderHint: response.LeaderHint,
				Response:   response.ClientID,
			}
		}

		if err != nil {
			c.IndentedJSON(http.StatusInternalServerError, requestData)
		} else {
			c.IndentedJSON(http.StatusOK, responseData)
		}
	})
}

type CommandType int

const (
	CommandTypeGet CommandType = iota
	CommandTypeSet
	CommandTypeRegister
)

var (
	get, _      = regexp.Compile(`^get\s[a-zA-A0-9\-\_]+$`)
	set, _      = regexp.Compile(`^set\s[a-zA-A0-9\-\_]+\s.+$`)
	register, _ = regexp.Compile(`^register$`)
)

func verifyRequest(request common.ClientRequestInput) (errs []error, cmdType CommandType) {
	cmd, ok := request.Command.(string)
	if !ok {
		errs = append(errs, errors.New("command must be a string"))
	}

	valid := false

	if get.MatchString(cmd) {
		valid = true
		cmdType = CommandTypeGet
	}

	if set.MatchString(cmd) {
		valid = true
		cmdType = CommandTypeSet
	}

	if register.MatchString(cmd) {
		valid = true
		cmdType = CommandTypeRegister
	}

	if !valid {
		errs = append(errs, errors.New("command is invalid"))
	}

	return
}

func (h HttpProxy) Start() {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	h.clientRequest(r)
	h.registerClient(r)
	h.clientQuery(r)
	h.cli(r)

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
