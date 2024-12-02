package http_proxy

import (
	"context"
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel/codes"

	. "khanh/raft-go/common/etcd"
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

func (h *EtcdHttpProxy) SetBrain(brain RaftBrain) {
	h.brain = brain
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

func NewEtcdHttpProxy(params NewEtcdHttpProxyParams) *EtcdHttpProxy {
	h := EtcdHttpProxy{
		host:       params.URL,
		stop:       make(chan struct{}),
		accessible: true,
		logger:     params.Logger,
	}

	return &h
}

func (h *EtcdHttpProxy) Start() {
	// gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	r.Use(otelgin.Middleware("gin-server"))
	r.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, DELETE")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	})

	h.prometheus(r)
	h.keyApi(r)
	r.GET("/", func(ctx *gin.Context) {
		ctx.String(200, "hello there")
	})

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

	h.log().InfoContext(ctx, "etcd HTTP server start")
	span.SetStatus(codes.Ok, "HTTP proxy start successfully")
}

func (h *EtcdHttpProxy) Stop() {
	select {
	case h.stop <- struct{}{}:
	default:
	}
}

func (h *EtcdHttpProxy) SetAccessible() {
	h.accessible = true
}

func (h *EtcdHttpProxy) SetInaccessible() {
	h.accessible = false
}

func (h *EtcdHttpProxy) log() observability.Logger {
	sub := h.logger.With(
		"origin", "EtcdHttpProxy",
	)

	return sub
}

func (h *EtcdHttpProxy) prometheus(r *gin.Engine) {
	r.GET("/v2/metrics", gin.WrapH(promhttp.Handler()))
}

func handleError(c *gin.Context, err error) {
	switch e := err.(type) {
	case common.RaftError:
		if e.HttpCode == http.StatusMovedPermanently {
			c.Header("Location", e.LeaderHint)
		}
		c.IndentedJSON(e.HttpCode, EtcdResultErr{
			Cause:   "raft",
			Message: err.Error(),
		})
	case EtcdResultErr:
		c.IndentedJSON(e.ErrorCode, err)
	default:
		panic(fmt.Sprintf("unknown error: %t, value: %v", e, e))
	}
}

func (h *EtcdHttpProxy) keyApi(r *gin.Engine) {
	r.PUT("/v2/keys/*key", func(c *gin.Context) {
		ctx, span := tracer.Start(c.Request.Context(), "etcd key api")
		defer span.End()

		reqArgs, err := parsePutRequest(c)
		if err != nil {
			c.IndentedJSON(http.StatusBadRequest, err)
			return
		}

		err = reqArgs.ValidatePut()
		if err != nil {
			c.IndentedJSON(http.StatusBadRequest, err)
			return
		}

		log := common.EtcdLog{
			Command: reqArgs.ToCommandPut(),
		}
		output := common.ClientRequestOutput{}
		err = h.brain.ClientRequest(ctx, log, &output)
		if err != nil {
			handleError(c, err)
		} else {
			res, ok := output.Response.(EtcdResult)
			if ok {
				if res.Promise == nil {
					c.IndentedJSON(200, res.Data)
				} else {
					data := <-res.Promise
					c.IndentedJSON(200, data)
				}
			} else {
				c.IndentedJSON(200, output)
			}
		}
	})

	r.DELETE("/v2/keys/*key", func(c *gin.Context) {
		ctx, span := tracer.Start(c.Request.Context(), "etcd key api")
		defer span.End()

		reqArgs, err := parseDeleteRequest(c)
		if err != nil {
			c.IndentedJSON(http.StatusBadRequest, err)
			return
		}

		err = reqArgs.ValidateDelete()
		if err != nil {
			c.IndentedJSON(http.StatusBadRequest, err)
			return
		}

		log := common.EtcdLog{
			Command: reqArgs.ToCommandDelete(),
		}
		output := common.ClientRequestOutput{}
		err = h.brain.ClientRequest(ctx, log, &output)
		if err != nil {
			handleError(c, err)
		} else {
			res, ok := output.Response.(EtcdResult)
			if ok {
				if res.Promise == nil {
					c.IndentedJSON(200, res.Data)
				} else {
					data := <-res.Promise
					c.IndentedJSON(200, data)
				}
			} else {
				c.IndentedJSON(200, output)
			}
		}
	})

	r.GET("/v2/keys/*key", func(c *gin.Context) {
		ctx, span := tracer.Start(c.Request.Context(), "etcd key api")
		defer span.End()

		reqArgs, err := parseGetRequest(c)
		if err != nil {
			c.IndentedJSON(http.StatusBadRequest, err)
			return
		}

		err = reqArgs.ValidateGet()
		if err != nil {
			c.IndentedJSON(http.StatusBadRequest, err)
			return
		}

		log := common.EtcdLog{
			Command: reqArgs.ToCommandGet(),
		}

		output := common.ClientQueryOutput{}
		err = h.brain.ClientQuery(ctx, log, &output)
		if err != nil {
			handleError(c, err)
		} else {
			res, ok := output.Response.(EtcdResult)
			if ok {
				if res.Promise == nil {
					c.IndentedJSON(200, res.Data)
				} else {
					data := <-res.Promise
					c.IndentedJSON(200, data)
				}
			} else {
				c.IndentedJSON(200, output)
			}
		}
	})
}
