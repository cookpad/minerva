package api

import (
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

var Logger = logrus.New()

type Arguments struct {
	DatabaseName     string
	IndexTableName   string
	MessageTableName string
	OutputPath       string
	Region           string
}

type apiResponse struct {
	Code    int
	Message interface{}
}

type handler func(args Arguments, c *gin.Context) (*apiResponse, apiError)

func handleRequest(args Arguments, c *gin.Context, hdlr handler) {
	resp, err := hdlr(args, c)
	if err != nil {
		c.JSON(err.Code(), gin.H{"message": err.Message()})
	} else {
		c.JSON(resp.Code, resp.Message)
	}
}

func SetupRoute(r *gin.RouterGroup, args Arguments) {
	r.POST("/search", func(c *gin.Context) {
		handleRequest(args, c, execSearch)
	})
	r.GET("/search/:query_id/result", func(c *gin.Context) {
		handleRequest(args, c, getSearchResult)
	})
}
