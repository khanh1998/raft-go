package http_proxy

import (
	"context"
	"errors"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"net/http"
	"regexp"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var (
	tracer = otel.Tracer("http-proxy")
)

type RaftBrain interface {
	// todo: remove returned error, error should be included in output
	ClientRequest(ctx context.Context, input common.Log, output *common.ClientRequestOutput) (err error)
	RegisterClient(ctx context.Context, input common.Log, output *common.RegisterClientOutput) (err error)
	KeepAlive(ctx context.Context, input common.Log, output *common.KeepAliveClientOutput) (err error)
	ClientQuery(ctx context.Context, input common.Log, output *common.ClientQueryOutput) (err error)
	AddServer(ctx context.Context, input common.Log, output *common.AddServerOutput) (err error)
	RemoveServer(ctx context.Context, input common.Log, output *common.RemoveServerOutput) (err error)
	GetInfo() common.GetStatusResponse
}

type HttpProxy struct {
	brain      RaftBrain
	host       string
	stop       chan struct{}
	accessible bool
	logger     observability.Logger
}

type NewHttpProxyParams struct {
	URL    string
	Logger observability.Logger
}

func NewHttpProxy(params NewHttpProxyParams) *HttpProxy {
	h := HttpProxy{
		host:       params.URL,
		stop:       make(chan struct{}),
		accessible: true,
		logger:     params.Logger,
	}

	return &h
}

func (h *HttpProxy) log() observability.Logger {
	sub := h.logger.With(
		"origin", "HttpProxy",
	)

	return sub
}

func (h *HttpProxy) Stop() {
	select {
	case h.stop <- struct{}{}:
	default:
	}
}

func (h *HttpProxy) SetAccessible() {
	h.accessible = true
}

func (h *HttpProxy) SetInaccessible() {
	h.accessible = false
}

func (h *HttpProxy) SetBrain(brain RaftBrain) {
	h.brain = brain
}
func (h *HttpProxy) prometheus(r *gin.Engine) {
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
}

func (h *HttpProxy) info(r *gin.Engine) {
	r.GET("/info", func(c *gin.Context) {
		info := h.brain.GetInfo()
		c.IndentedJSON(200, info)
	})
}

func (h *HttpProxy) cli(r *gin.Engine) {
	r.POST("/cli", func(c *gin.Context) {
		ctx, span := tracer.Start(c.Request.Context(), "CliHandler")
		defer span.End()

		if !h.accessible {
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
			errStr := ""
			for _, err := range errs {
				errStr += err.Error() + ";"
			}

			responseData = common.ClientRequestOutput{
				Status:     "Not OK",
				Response:   errStr,
				LeaderHint: "",
			}

			c.IndentedJSON(http.StatusBadRequest, responseData)

			return
		}

		span.SetAttributes(
			attribute.Int("sequence", requestData.SequenceNum),
			attribute.Int("clientID", requestData.ClientID),
			attribute.String("command", requestData.Command),
		)

		var (
			err error
		)

		switch cmdType {
		case CommandTypeGet:
			var response common.ClientQueryOutput

			log := common.ClassicLog{
				Command: requestData.Command,
			}

			err = h.brain.ClientQuery(ctx, log, &response)
			responseData = common.ClientRequestOutput{
				Status:     response.Status,
				Response:   response.Response,
				LeaderHint: response.LeaderHint,
			}
		case CommandTypeSet, CommandTypeDel:
			log := common.ClassicLog{
				ClientID:    requestData.ClientID,
				SequenceNum: requestData.SequenceNum,
				Command:     requestData.Command,
			}
			var response common.ClientRequestOutput
			err = h.brain.ClientRequest(ctx, log, &response)
			responseData = common.ClientRequestOutput{
				Status:     response.Status,
				Response:   response.Response,
				LeaderHint: response.LeaderHint,
			}
		case CommandTypeRegister:
			var response common.RegisterClientOutput
			log := common.ClassicLog{
				Command: "register",
			}
			err = h.brain.RegisterClient(ctx, log, &response)
			responseData = common.ClientRequestOutput{
				Status:     response.Status,
				LeaderHint: response.LeaderHint,
				Response:   response.Response,
			}
		case CommandTypeKeepAlive:
			log := common.ClassicLog{
				ClientID:    requestData.ClientID,
				SequenceNum: requestData.SequenceNum,
				Command:     "keep-alive",
			}
			var response common.KeepAliveClientOutput
			err = h.brain.KeepAlive(ctx, log, &response)
			responseData = common.ClientRequestOutput{
				Status:     response.Status,
				LeaderHint: response.LeaderHint,
				Response:   response.Response,
			}
		case CommandTypeAddServer:
			log := common.ClassicLog{
				Command: requestData.Command,
			}
			var response common.AddServerOutput
			err = h.brain.AddServer(ctx, log, &response)
			responseData = common.ClientRequestOutput{
				Status:     response.Status,
				LeaderHint: response.LeaderHint,
				Response:   response.Response,
			}
		case CommandTypeRemoveServer:
			log := common.ClassicLog{
				Command: requestData.Command,
			}
			var response common.RemoveServerOutput
			err = h.brain.RemoveServer(ctx, log, &response)
			responseData = common.ClientRequestOutput{
				Status:     response.Status,
				LeaderHint: response.LeaderHint,
				Response:   response.Response,
			}
		}

		if err != nil {
			c.String(http.StatusInternalServerError, err.Error())
		} else {
			c.IndentedJSON(http.StatusOK, responseData)
		}
	})
}

type CommandType int

const (
	CommandTypeGet CommandType = iota
	CommandTypeSet
	CommandTypeDel
	CommandTypeRegister
	CommandTypeKeepAlive
	CommandTypeAddServer
	CommandTypeRemoveServer
)

var (
	get, _          = regexp.Compile(`^get\s[a-zA-A0-9\-\_]+$`)
	set, _          = regexp.Compile(`^set\s[a-zA-A0-9\-\_]+\s.+$`)
	del, _          = regexp.Compile(`^del\s[a-zA-A0-9\-\_]+$`)
	register, _     = regexp.Compile(`^register$`)
	keepAlive, _    = regexp.Compile(`^keep-alive$`)
	addServer, _    = regexp.Compile(`^addServer\s[0-9]+\s.+\s.+$`)
	removeServer, _ = regexp.Compile(`^removeServer\s[0-9]+\s.+\s.+$`)
)

func verifyRequest(request common.ClientRequestInput) (errs []error, cmdType CommandType) {
	cmd := request.Command
	valid := false

	if get.MatchString(cmd) {
		valid = true
		cmdType = CommandTypeGet
	}

	if set.MatchString(cmd) {
		valid = true
		cmdType = CommandTypeSet
	}

	if del.MatchString(cmd) {
		valid = true
		cmdType = CommandTypeDel
	}

	if register.MatchString(cmd) {
		valid = true
		cmdType = CommandTypeRegister
	}

	if keepAlive.MatchString(cmd) {
		valid = true
		cmdType = CommandTypeKeepAlive
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
	r.Use(otelgin.Middleware("gin-server"))
	r.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	})

	h.cli(r)
	h.prometheus(r)
	h.info(r)

	httpServer := &http.Server{
		Addr:    h.host,
		Handler: r,
	}

	ctx, span := tracer.Start(context.Background(), "Start HTTP proxy")
	defer span.End()

	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			h.log().ErrorContext(ctx, "HTTP Proxy Start", err)
			span.SetStatus(codes.Error, err.Error())
		}
	}()

	go func() {
		<-h.stop
		httpServer.Shutdown(context.Background())
		h.log().InfoContext(context.Background(), "HTTP Proxy stop")
	}()

	h.log().InfoContext(ctx, "HTTP server start")
	span.SetStatus(codes.Ok, "HTTP proxy start successfully")
}
