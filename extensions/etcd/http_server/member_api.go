package http_server

import (
	gc "khanh/raft-go/common"
	"khanh/raft-go/extensions/etcd/common"
	"net"
	"strconv"

	"github.com/gin-gonic/gin"
)

func checkUrl(inputUrl string) error {
	_, _, err := net.SplitHostPort(inputUrl)
	if err != nil {
		return err
	}
	return nil
}

func (h *EtcdHttpProxy) memberApi(r *gin.Engine) {
	r.GET("/v2/members", func(c *gin.Context) {
		members := h.brain.GetMembers()
		c.IndentedJSON(200, members)
	})

	r.POST("/v2/members/:serverId", func(c *gin.Context) {
		value := c.Param("serverId")
		serverId, err := strconv.Atoi(value)
		if err != nil {
			err = common.CreateError("serverId", err)
			h.handleError(c, err)
		}
		httpUrl := c.Query("httpUrl")
		rpcUrl := c.Query("rpcUrl")

		ctx := c.Request.Context()
		log := common.EtcdLogFactory{}.ComposeChangeServerCommand(true, serverId, httpUrl, rpcUrl)

		var output gc.AddServerOutput
		err = h.brain.AddServer(ctx, log, &output)
		if err != nil {
			h.handleError(c, err)
		} else {
			h.handleResult(c, output.Response)
		}
	})

	r.DELETE("/v2/members/:serverId", func(c *gin.Context) {
		value := c.Param("serverId")
		serverId, err := strconv.Atoi(value)
		if err != nil {
			err = common.CreateError("serverId", err)
			h.handleError(c, err)
		}
		httpUrl := c.Query("httpUrl")
		rpcUrl := c.Query("rpcUrl")

		ctx := c.Request.Context()
		log := common.EtcdLogFactory{}.ComposeChangeServerCommand(false, serverId, httpUrl, rpcUrl)

		var output gc.RemoveServerOutput
		err = h.brain.RemoveServer(ctx, log, &output)
		if err != nil {
			h.handleError(c, err)
		} else {
			h.handleResult(c, output.Response)
		}
	})
}
