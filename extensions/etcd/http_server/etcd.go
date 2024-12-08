package http_server

import (
	"context"
	"fmt"
	gc "khanh/raft-go/common"
	"khanh/raft-go/extensions/etcd/common"
	"khanh/raft-go/observability"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
)

type RaftBrain interface {
	// todo: remove returned error, error should be included in output
	ClientRequest(ctx context.Context, input gc.Log, output *gc.ClientRequestOutput) (err error)
	RegisterClient(ctx context.Context, input gc.Log, output *gc.RegisterClientOutput) (err error)
	KeepAlive(ctx context.Context, input gc.Log, output *gc.KeepAliveClientOutput) (err error)
	ClientQuery(ctx context.Context, input gc.Log, output *gc.ClientQueryOutput) (err error)
	AddServer(ctx context.Context, input gc.Log, output *gc.AddServerOutput) (err error)
	RemoveServer(ctx context.Context, input gc.Log, output *gc.RemoveServerOutput) (err error)
	GetInfo() gc.GetStatusResponse
	GetMembers() []gc.ClusterMember
}

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

var tracer = otel.Tracer("main.go")

func NewEtcdHttpProxy(params NewEtcdHttpProxyParams) *EtcdHttpProxy {
	h := EtcdHttpProxy{
		host:       params.URL,
		stop:       make(chan struct{}),
		accessible: true,
		logger:     params.Logger,
	}

	return &h
}

func CustomLogger(log observability.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Before the request
		startTime := time.Now()

		// Process the request
		c.Next()

		// After the request
		duration := time.Since(startTime)
		statusCode := c.Writer.Status()

		dumpReq, _ := httputil.DumpRequest(c.Request, true)

		log.Info(
			"Gin:",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", statusCode,
			"duration", duration,
			"request", string(dumpReq),
		)
	}
}

func (h *EtcdHttpProxy) Start() {
	// gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(CustomLogger(h.log()))

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
	h.memberApi(r)
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

func (h *EtcdHttpProxy) info(r *gin.Engine) {
	r.GET("/v2/info", func(ctx *gin.Context) {
		data := h.brain.GetInfo()
		ctx.IndentedJSON(200, data)
	})
}

// GenerateRedirectURL creates a full URL for the Location header based on the target host and the current path.
func GenerateRedirectURL(c *gin.Context, targetHost string) string {
	// Get the current request's protocol (http/https)
	protocol := "http"
	if c.Request.TLS != nil {
		protocol = "https"
	}
	// Capture the original query parameters
	query := c.Request.URL.RawQuery
	// Get the current request's path
	currentPath := c.Request.URL.Path

	// Construct the full URL using the target host and the current path
	redirectURL := fmt.Sprintf("%s://%s%s", protocol, targetHost, currentPath)
	if query != "" {
		redirectURL += "?" + query
	}

	// Parse the URL to ensure it's valid (optional, for validation purposes)
	parsedURL, err := url.Parse(redirectURL)
	if err != nil {
		// Handle error (you can return a default value or log the error)
		fmt.Println("Error parsing URL:", err)
		return ""
	}

	// Return the valid redirect URL
	return parsedURL.String()
}

func (h *EtcdHttpProxy) handleResult(c *gin.Context, res gc.LogResult) {
	raft := h.brain.GetInfo()
	c.Header("X-Raft-Index", strconv.Itoa(raft.CommitIndex))
	c.Header("X-Raft-Term", strconv.Itoa(raft.Term))

	switch e := res.(type) {
	case common.EtcdResult:
		if e.Promise == nil {
			c.Header("X-Etcd-Index", strconv.Itoa(e.Data.ChangeIndex))
			c.IndentedJSON(200, e.Data)
		} else {
			data := <-e.Promise
			c.Header("X-Etcd-Index", strconv.Itoa(data.ChangeIndex))
			c.IndentedJSON(200, data)
		}
	default:
		c.IndentedJSON(200, res)
	}
}

func (h *EtcdHttpProxy) handleError(c *gin.Context, err error) {
	raft := h.brain.GetInfo()
	c.Header("X-Raft-Index", strconv.Itoa(raft.CommitIndex))
	c.Header("X-Raft-Term", strconv.Itoa(raft.Term))

	switch e := err.(type) {
	case gc.RaftError:
		if e.HttpCode >= 300 && e.HttpCode < 400 {
			c.Header("Location", GenerateRedirectURL(c, e.LeaderHint))
		}
		c.IndentedJSON(e.HttpCode, common.EtcdResultErr{
			Cause:   "raft",
			Message: err.Error(),
		})
	case common.EtcdResultErr:
		c.Header("X-Etcd-Index", strconv.Itoa(e.Index))
		c.IndentedJSON(e.ErrorCode, err)
	default:
		panic(fmt.Sprintf("unknown error: %t, value: %v", e, e))
	}
}
