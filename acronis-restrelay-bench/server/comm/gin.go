package comm

import (
	"compress/gzip"
	"io"
	"net/http"

	ginGzip "github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"

	"restrelay-bench/server/logic"
	"restrelay-bench/server/version"
)

type ginHTTPServer struct{}

func (srv *ginHTTPServer) ServerListenAndServe(addr string, baseURL string, nodeName string) error {
	r := gin.Default()
	r.Use(ginGzip.Gzip(gzip.DefaultCompression))

	var cb = func(c *gin.Context) {
		tb := logic.NewTimestampBuilder(nodeName)

		tb.AddTimestamp()

		correlationID := c.Request.Header.Get("X-Correlation-ID")
		if correlationID == "" {
			correlationID = newCorrelationID()
		}

		requestWrapper := logic.RequestWrapper{
			Args: StandardURIArgs{
				Values: c.Request.URL.Query(),
			},
			URI:              c.Request.URL.RequestURI(),
			ConnectionHeader: c.Request.Header.Get("Connection"),
		}

		reader, headers, status, err := logic.PerformActions(baseURL, requestWrapper, nodeName, &tb, correlationID)
		if err != nil {
			c.String(http.StatusInternalServerError, err.Error())

			return
		}
		content, err := io.ReadAll(reader)
		if err != nil {
			c.String(http.StatusInternalServerError, err.Error())

			return
		}

		c.Header("Server", "gin")
		for key, value := range headers {
			c.Header(key, value)
		}

		c.String(status, string(content))
	}

	r.GET("/api/restrelay_bench_server/", cb)
	r.GET("/api/restrelay_bench_server/version", func(c *gin.Context) {
		c.String(http.StatusOK, version.Version)
	})
	r.GET("/api/restrelay_bench_server/type", func(c *gin.Context) {
		c.String(http.StatusOK, version.Type)
	})

	return r.Run(addr) //nolint:wrapcheck
}
