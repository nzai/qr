package api

import (
	"net/http"

	_ "net/http/pprof"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// Server api server
type Server struct {
	engine *gin.Engine
}

// NewServer create api server
func NewServer() *Server {
	gin.SetMode(gin.ReleaseMode)
	server := &Server{
		engine: gin.New(),
	}

	zap.L().Debug("init gin success")

	server.engine.Use(server.logger(), server.recovery())

	pprof.Register(server.engine, "/v1/pprof")

	server.registeRoute()

	zap.L().Debug("register route success")

	return server
}

func (s Server) Run() error {
	zap.L().Info("listen address: 0.0.0.0:21000")
	return s.engine.Run(":21000")
}

func (s Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.engine.ServeHTTP(w, r)
}
