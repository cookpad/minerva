package api

import (
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// Logger is exported to be used in external package.
var Logger = logrus.New()

// SetupRoute binds route of gin and API
func SetupRoute(r *gin.RouterGroup, handler Handler) {
	r.POST("/search", func(c *gin.Context) {
		resp, err := handler.ExecSearch(c)
		sendResponse(c, resp, err)
	})
	r.GET("/search/:query_id/logs", func(c *gin.Context) {
		resp, err := handler.GetSearchLogs(c)
		sendResponse(c, resp, err)
	})
	r.GET("/search/:query_id/timeseries", func(c *gin.Context) {
		resp, err := handler.GetSearchTimeSeries(c)
		sendResponse(c, resp, err)
	})
}
