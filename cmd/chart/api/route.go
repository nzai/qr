package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func (s Server) registeRoute() {
	s.engine.NoRoute(func(c *gin.Context) {
		c.AbortWithStatus(http.StatusNotFound)
	})

	s.engine.StaticFS("/page", http.Dir("page"))

	s.engine.GET("/", func(c *gin.Context) {
		c.File("page/index.html")

	})

	s.engine.GET("/api/ping", s.ping)

	s.engine.GET("/api/data", s.getSerial)

}

// Ping godoc
// @ID ping
// @Summary Ping
// @Description Ping and test service
// @Tags common
// @Accept  json
// @Produce  plain
// @Success 200 {object} string
// @Failure 400 {object} BadRequestResponse
// @Failure 404 {object} NotFoundResponse
// @Failure 500 {object} InternalServerErrorResponse
// @Router /ping [get]
func (s Server) ping(c *gin.Context) {
	c.String(http.StatusOK, "pong")
}
