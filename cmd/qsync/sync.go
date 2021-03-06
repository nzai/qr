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

	go func(_wg *sync.WaitGroup) {
		for _, exchange := range s.exchanges {
			zap.L().Info("sync exchange daily quote start",
				zap.String("exchange", exchange.Code()))

			err := s.syncExchange(exchange)
			if err != nil {
				zap.L().Error("sync exchange daily quote failed",
					zap.Error(err),
					zap.String("exchange", exchange.Code()))
			} else {
				zap.L().Info("sync exchange daily quote finished",
					zap.String("exchange", exchange.Code()))
			}

			_wg.Done()
		}
	}(wg)

	return wg
}

// syncExchange sync exchange quote
func (s Sync) syncExchange(exchange exchanges.Exchange) error {
	startTime := time.Now()

	date := time.Date(2015, 5, 1, 0, 0, 0, 0, exchange.Location())
	endDate := utils.TodayZero(startTime.In(exchange.Location()))

	total := int(endDate.Sub(date).Hours() / 24)
	var processed int

	for date.Before(endDate) {
		zap.L().Info(fmt.Sprintf("(%.2f%%) start", float64(processed)*100/float64(total)),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date),
			zap.String("d", date.Weekday().String()),
			zap.String("process", fmt.Sprintf("%d/%d", processed, total)))

		start := time.Now()

		exists, err := s.syncExchangeDate(exchange, date)
		if err != nil {
			return err
		}

		if exists {
			total--
		} else {
			processed++

			passed := time.Now().Sub(startTime)
			speed := float64(processed) / passed.Seconds()
			remain := time.Duration(float64(total-processed)/speed) * time.Second

			zap.L().Info(fmt.Sprintf("(%.2f%%) synced", float64(processed)*100/float64(total)),
				zap.String("exchange", exchange.Code()),
				zap.Time("date", date),
				zap.Duration("d", time.Now().Sub(start)),
				zap.String("process", fmt.Sprintf("%d/%d", processed, total)),
				zap.Duration("remain", remain))
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
			if index > 0 {
				zap.L().Info("sync exchange daily quote success",
					zap.String("exchange", exchange.Code()),
					zap.Time("date", date),
					zap.String("retries", fmt.Sprintf("%d/%d", index+1, constants.RetryCount)))
			}
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

	start := time.Now()
	edq, err := s.source.Load(exchange, date)
	if err != nil {
		zap.L().Error("load exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return false, err
	}
	zap.L().Info("load source success",
		zap.Duration("d", time.Now().Sub(start)),
		zap.String("exchange", exchange.Code()),
		zap.Time("date", date))

	start = time.Now()
	err = s.dest.Save(exchange, date, edq)
	if err != nil {
		zap.L().Error("save exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return false, err
	}
	zap.L().Info("save dest success",
		zap.Duration("d", time.Now().Sub(start)),
		zap.String("exchange", exchange.Code()),
		zap.Time("date", date))

	start = time.Now()
	// validate
	saved, err := s.dest.Load(exchange, date)
	if err != nil {
		zap.L().Error("load saved exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return false, err
	}
	zap.L().Info("load dest success",
		zap.Duration("d", time.Now().Sub(start)),
		zap.String("exchange", exchange.Code()),
		zap.Time("date", date))

	err = saved.Equal(*edq)
	if err != nil {
		zap.L().Error("saved exchange daily quote is different",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))

		err1 := s.dest.Delete(exchange, date)
		if err1 != nil {
			zap.L().Error("delete dest exchange daily quote failed",
				zap.Error(err1),
				zap.String("exchange", exchange.Code()),
				zap.Time("date", date))
			return false, err1
		}

		return false, err
	}

	return false, nil
}
