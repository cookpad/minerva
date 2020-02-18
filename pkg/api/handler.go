package api

import (
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// Response is
type Response struct {
	Code    int
	Message interface{}
}

type Handler interface {
	ExecSearch(c *gin.Context) (*Response, Error)
	GetSearchLogs(c *gin.Context) (*Response, Error)
	GetSearchTimeSeries(c *gin.Context) (*Response, Error)
}

type MinervaHandler struct {
	DatabaseName     string
	IndexTableName   string
	MessageTableName string
	OutputPath       string
	Region           string
}

// Handler is handler interface
func sendResponse(c *gin.Context, resp *Response, err Error) {
	var code int
	if resp != nil {
		code = resp.Code
	}

	Logger.WithFields(logrus.Fields{
		"path":       c.FullPath(),
		"request_id": c.GetHeader("x-request-id"),
		"ipaddr":     c.ClientIP(),
		"user_agent": c.Request.UserAgent(),
		"resp_code":  code,
		"error":      err,
	}).Info("Audit log")

	if err != nil {
		Logger.WithFields(logrus.Fields{
			"error":  err,
			"params": c.Params,
			"url":    c.Request.URL,
		}).Error("Request faield")
		c.JSON(err.Code(), gin.H{"message": err.Message()})
	} else {
		c.JSON(resp.Code, resp.Message)
	}
}
