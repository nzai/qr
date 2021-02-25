package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/nzai/qr/cmd/chart/entity"
	"github.com/nzai/qr/cmd/chart/model"
	"go.uber.org/zap"
)

func (s Server) getSerial(c *gin.Context) {
	yr, _, err := model.GetYahooQuoteDownloader().DailyOfYear("AAPL", 2020)
	if err != nil {
		zap.L().Error("get quote failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, Response{Error: err.Error()})
		return
	}

	c.JSON(http.StatusOK, Response{Data: entity.ChartData{
		Quotes: yr.ToAdjQuotes().Slice(),
	}})
}
