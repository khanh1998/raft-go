package logic

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func (n NodeImpl) initApi(url string) {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.POST("/data", func(c *gin.Context) {
		n.log().Info().Msg("GIN handler receive a request from client")
		var request ClientRequest
		if err := c.BindJSON(&request); err != nil {
			return
		}

		n.ClientRequests <- request
		c.IndentedJSON(http.StatusOK, "ok babe")
	})

	go func() {
		r.Run(url)
	}()
}
