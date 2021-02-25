package main

import (
	"fmt"
	"os"

	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	if len(os.Args) < 2 {
		fmt.Println("usage: indicator {code}")
		return
	}

	quotes, err := NewYahooQuoteDownloader().DailyAll(os.Args[1])
	if err != nil {
		zap.L().Fatal("download quote failed", zap.Error(err))
	}

	zap.L().Info("download quote success", zap.Any("quotes", len(quotes.Timestamp)))
}
