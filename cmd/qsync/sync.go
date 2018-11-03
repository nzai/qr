package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/stores"
	"github.com/nzai/qr/utils"
	"go.uber.org/zap"
)

// Sync sync exchange daily quote
type Sync struct {
	source    stores.Store
	dest      stores.Store
	exchanges []exchanges.Exchange
}

// NewSync create new exchange daily quote sync
func NewSync(source stores.Store, dest stores.Store, exchanges []exchanges.Exchange) *Sync {
	return &Sync{source: source, dest: dest, exchanges: exchanges}
}

// Run sync jobs
func (s Sync) Run() *sync.WaitGroup {
	wg := new(sync.WaitGroup)
	wg.Add(len(s.exchanges))

	for _, exchange := range s.exchanges {
		go func(_exchange exchanges.Exchange, _wg *sync.WaitGroup) {
			defer _wg.Done()
			zap.L().Info("sync exchange daily quote start",
				zap.String("exchange", _exchange.Code()))

			err := s.syncExchange(_exchange)
			if err != nil {
				zap.L().Error("sync exchange daily quote failed",
					zap.Error(err),
					zap.String("exchange", _exchange.Code()))
				return
			}

			zap.L().Info("sync exchange daily quote finished",
				zap.String("exchange", _exchange.Code()))
		}(exchange, wg)
	}

	return wg
}

// syncExchange sync exchange quote
func (s Sync) syncExchange(exchange exchanges.Exchange) error {
	startTime := time.Now()

	date := time.Date(2015, 5, 1, 0, 0, 0, 0, exchange.Location())
	endDate := utils.TodayZero(startTime.In(exchange.Location()))

	total := int(endDate.Sub(date).Hours() / 24)
	processed := 0

	for date.Before(endDate) {
		exists, err := s.syncExchangeDate(exchange, date)
		if err != nil {
			return err
		}

		if exists {
			total--
		} else {
			processed++

			passed := time.Now().Sub(startTime)
			qps := float64(0)
			speed := float64(processed) / passed.Seconds()

			if speed != 0 {
				qps = float64(1) / speed
			}
			remain := time.Duration(float64(total-processed)/speed) * time.Second

			zap.L().Info(fmt.Sprintf("(%.2f%%) synced", float64(processed)*100/float64(total)),
				zap.String("exchange", exchange.Code()),
				zap.Time("date", date),
				zap.String("d", date.Weekday().String()),
				zap.String("process", fmt.Sprintf("%d/%d", processed, total)),
				zap.Float64("qps", qps),
				zap.Duration("remain", remain),
			)
		}

		date = date.AddDate(0, 0, 1)
	}

	return nil
}

// syncExchangeDate sync exchange daily quote
func (s Sync) syncExchangeDate(exchange exchanges.Exchange, date time.Time) (bool, error) {
	exists := false
	var err error
	for index := 0; index < constants.RetryCount; index++ {
		exists, err = s.syncExchangeDateOnce(exchange, date)
		if err == nil {
			return exists, nil
		}

		if index < constants.RetryCount-1 {
			zap.L().Warn("sync exchange daily quote failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Time("date", date),
				zap.Duration("retry in", constants.RetryInterval),
				zap.String("retries", fmt.Sprintf("%d/%d", index+1, constants.RetryCount)))
			time.Sleep(constants.RetryInterval)
		}
	}

	return false, err
}

// syncExchangeDateOnce sync exchange daily quote once
func (s Sync) syncExchangeDateOnce(exchange exchanges.Exchange, date time.Time) (bool, error) {
	exists, err := s.dest.Exists(exchange, date)
	if err != nil {
		zap.L().Error("check dest exchange daily quote exists failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return true, err
	}

	if exists {
		return true, nil
	}

	exists, err = s.source.Exists(exchange, date)
	if err != nil {
		zap.L().Error("check source exchange daily quote exists failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return true, err
	}

	if !exists {
		return false, nil
	}

	edq, err := s.source.Load(exchange, date)
	if err != nil {
		zap.L().Error("load exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return false, err
	}

	err = s.dest.Save(exchange, date, edq)
	if err != nil {
		zap.L().Error("save exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return false, err
	}

	// validate
	saved, err := s.dest.Load(exchange, date)
	if err != nil {
		zap.L().Error("load saved exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return false, err
	}

	err = saved.Equal(*edq)
	if err != nil {
		zap.L().Error("saved exchange daily quote is different",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return false, err
	}

	return false, nil
}
