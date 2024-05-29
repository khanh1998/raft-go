package http_proxy

import (
	"context"
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"net/http"
	"regexp"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

type RaftBrain interface {
	// todo: remove returned error, error should be included in output
	ClientRequest(input *common.ClientRequestInput, output *common.ClientRequestOutput) (err error)
	RegisterClient(input *common.RegisterClientInput, output *common.RegisterClientOutput) (err error)
	ClientQuery(input *common.ClientQueryInput, output *common.ClientQueryOutput) (err error)
	AddServer(input common.AddServerInput, output *common.AddServerOutput) (err error)
	RemoveServer(input common.RemoveServerInput, output *common.RemoveServerOutput) (err error)
}

type HttpProxy struct {
	brain      RaftBrain
	host       string
	Stop       chan struct{}
	Accessible bool
}

type NewHttpProxyParams struct {
	URL string
}

func NewHttpProxy(params NewHttpProxyParams) *HttpProxy {
	h := HttpProxy{host: params.URL, Stop: make(chan struct{}), Accessible: true}

	return &h
}

func (h *HttpProxy) SetAccessible() {
	h.Accessible = true
}

func (h *HttpProxy) SetInaccessible() {
	h.Accessible = false
}

func (h *HttpProxy) SetBrain(brain RaftBrain) {
	h.brain = brain
}

func (h *HttpProxy) cli(r *gin.Engine) {
	r.POST("/cli", func(c *gin.Context) {
		fmt.Println("receive a request ", h.Accessible)
		if !h.Accessible {
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
		case CommandTypeAddServer:
			var (
				httpUrl, rpcUrl string
				id              int
			)
			id, httpUrl, rpcUrl, err = common.DecomposeAddServerCommand(requestData.Command.(string))
			if err != nil {
				responseData = common.ClientRequestOutput{
					Status:   common.StatusNotOK,
					Response: "invalid command",
				}
			} else {
				request := common.AddServerInput{
					ID:               id,
					NewServerHttpUrl: httpUrl,
					NewServerRpcUrl:  rpcUrl,

					ClientID:    requestData.ClientID,
					SequenceNum: requestData.SequenceNum,
				}
				var response common.AddServerOutput
				err = h.brain.AddServer(request, &response)
				responseData = common.ClientRequestOutput{
					Status:     response.Status,
					LeaderHint: response.LeaderHint,
					Response:   response.Response,
				}
			}
		case CommandTypeRemoveServer:
			var (
				httpUrl, rpcUrl string
				id              int
			)
			id, httpUrl, rpcUrl, err = common.DecomposeRemoveServerCommand(requestData.Command.(string))
			if err != nil {
				responseData = common.ClientRequestOutput{
					Status:   common.StatusNotOK,
					Response: "invalid command",
				}
			} else {
				request := common.RemoveServerInput{
					ID:               id,
					NewServerHttpUrl: httpUrl,
					NewServerRpcUrl:  rpcUrl,

					ClientID:    requestData.ClientID,
					SequenceNum: requestData.SequenceNum,
				}
				var response common.RemoveServerOutput
				err = h.brain.RemoveServer(request, &response)
				responseData = common.ClientRequestOutput{
					Status:     response.Status,
					LeaderHint: response.LeaderHint,
					Response:   response.Response,
				}
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
	CommandTypeAddServer
	CommandTypeRemoveServer
)

var (
	get, _          = regexp.Compile(`^get\s[a-zA-A0-9\-\_]+$`)
	set, _          = regexp.Compile(`^set\s[a-zA-A0-9\-\_]+\s.+$`)
	register, _     = regexp.Compile(`^register$`)
	addServer, _    = regexp.Compile(`^addServer\s[0-9]+\s.+\s.+$`)
	removeServer, _ = regexp.Compile(`^removeServer\s[0-9]+\s.+\s.+$`)
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

	if addServer.MatchString(cmd) {
		valid = true
		cmdType = CommandTypeAddServer
	}

	if removeServer.MatchString(cmd) {
		valid = true
		cmdType = CommandTypeRemoveServer
	}

	if !valid {
		errs = append(errs, errors.New("command is invalid"))
	}

	return
}

func (h *HttpProxy) Start() {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	fmt.Println(h)

	h.cli(r)

	httpServer := &http.Server{
		Addr:    h.host,
		Handler: r,
	}

	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			log.Err(err).Msg("HTTP Proxy Start")
		}
	}()

	go func() {
		<-h.Stop
		httpServer.Shutdown(context.Background())
		log.Info().Msg("HTTP Proxy stop")
	}()

	log.Info().Msg("HTTP start")
}
