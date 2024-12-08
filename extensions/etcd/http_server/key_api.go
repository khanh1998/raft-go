package http_server

import (
	gc "khanh/raft-go/common"
	"khanh/raft-go/extensions/etcd/common"
	"net/http"

	"github.com/gin-gonic/gin"
)

func (h *EtcdHttpProxy) keyApi(r *gin.Engine) {
	r.PUT("/v2/keys/*key", func(c *gin.Context) {
		ctx, span := tracer.Start(c.Request.Context(), "etcd key api")
		defer span.End()

		reqArgs, err := parsePutRequest(c)
		if err != nil {
			h.handleError(c, err)
			return
		}

		err = reqArgs.ValidatePut()
		if err != nil {
			h.handleError(c, err)
			return
		}

		log := common.EtcdLog{
			Command: reqArgs.ToCommandPut(),
		}

		output := gc.ClientRequestOutput{}
		err = h.brain.ClientRequest(ctx, log, &output)
		if err != nil {
			h.handleError(c, err)
		} else {
			h.handleResult(c, output.Response)
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

		output := gc.ClientRequestOutput{}
		err = h.brain.ClientRequest(ctx, log, &output)
		if err != nil {
			h.handleError(c, err)
		} else {
			h.handleResult(c, output.Response)
		}
	})

	r.GET("/v2/keys/*key", func(c *gin.Context) {
		ctx, span := tracer.Start(c.Request.Context(), "etcd key api")
		defer span.End()

		reqArgs, err := parseGetRequest(c)
		if err != nil {
			h.handleError(c, err)
			return
		}

		err = reqArgs.ValidateGet()
		if err != nil {
			h.handleError(c, err)
			return
		}

		log := common.EtcdLog{
			Command: reqArgs.ToCommandGet(),
		}

		h.log().InfoContext(ctx, "/v2/keys/*key", "log", log)

		output := gc.ClientQueryOutput{}
		err = h.brain.ClientQuery(ctx, log, &output)
		h.log().InfoContext(ctx, "/v2/keys/*key", "output", output)
		if err != nil {
			h.handleError(c, err)
		} else {
			h.handleResult(c, output.Response)
		}
	})
}
