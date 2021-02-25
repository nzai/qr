package api

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/gin-gonic/gin"
)

func (s Server) logger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		requstURL := c.Request.URL.String()

		fields := []zap.Field{
			zap.String("type", "logger"),
			zap.String("method", c.Request.Method),
			zap.String("url", requstURL),
			zap.String("viewer_ip", c.GetHeader("X-Forwarded-For")),
			zap.String("viewer_country", c.GetHeader("Cloudfront-Viewer-Country")),
			zap.String("viewer_timezone", c.GetHeader("CloudFront-Viewer-Time-Zone")),
		}

		zap.L().Info(fmt.Sprintf("[START] %s %s", c.Request.Method, requstURL), fields...)

		// Process request
		c.Next()

		// add response fields
		duration := time.Since(start)
		fields = append(fields,
			zap.Int("size", c.Writer.Size()),
			zap.Int("status", c.Writer.Status()),
			zap.Int64("duration", duration.Milliseconds()))

		fn := zap.L().Info
		if duration > time.Second*10 {
			fn = zap.L().Warn
		}

		if c.Writer.Status() == http.StatusInternalServerError {
			fn = zap.L().Error
		}

		fn(fmt.Sprintf("[END] %s %s (%d) in %s", c.Request.Method, requstURL, c.Writer.Status(), duration.String()), fields...)
	}
}

// recovery returns a middleware for a given writer that recovers from any panics and writes a 500 if there was one.
func (s Server) recovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				// Check for a broken connection, as it is not really a
				// condition that warrants a panic stack trace.
				var brokenPipe bool
				if ne, ok := err.(*net.OpError); ok {
					if se, ok := ne.Err.(*os.SyscallError); ok {
						if strings.Contains(strings.ToLower(se.Error()), "broken pipe") || strings.Contains(strings.ToLower(se.Error()), "connection reset by peer") {
							brokenPipe = true
						}
					}
				}

				zap.L().Error("[Recovery] panic recovered",
					zap.Stack("stack"),
					zap.String("type", "recovery"),
					zap.String("method", c.Request.Method),
					zap.String("url", c.Request.URL.String()),
					zap.Int64("content_length", c.Request.ContentLength),
					zap.String("viewer_ip", c.GetHeader("X-Forwarded-For")),
					zap.String("viewer_country", c.GetHeader("Cloudfront-Viewer-Country")),
					zap.String("viewer_timezone", c.GetHeader("CloudFront-Viewer-Time-Zone")),
				)

				// If the connection is dead, we can't write a status to it.
				if brokenPipe {
					c.Error(err.(error)) // nolint: errcheck
					c.Abort()
				} else {
					c.AbortWithStatus(http.StatusInternalServerError)
				}
			}
		}()
		c.Next()
	}
}
