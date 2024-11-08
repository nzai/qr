package schedulers

import (
	"fmt"
	"sync"
	"time"

	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/notifiers"
	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/stores"
	"github.com/nzai/qr/utils"
	"go.uber.org/zap"
)

// Scheduler define a crawl scheduler
type Scheduler struct {
	store stores.Store
	// notifier  notifiers.Notifier
	exchanges []exchanges.Exchange
	limiter   *Limiter
}

// NewScheduler create crawl scheduler
func NewScheduler(store stores.Store, exchanges ...exchanges.Exchange) *Scheduler {
	return &Scheduler{
		store: store,
		// notifier:  notifier,
		exchanges: exchanges,
		limiter:   NewLimiter(constants.DefaultParallel),
	}
}

// Run jobs
func (s Scheduler) Run(start time.Time) *sync.WaitGroup {
	wg := new(sync.WaitGroup)

	for _, exchange := range s.exchanges {
		wg.Add(2)

		// start history job
		go s.historyJob(wg, exchange, start)

		// start daily job
		go s.dailyJob(wg, exchange)
	}

	return wg
}

// historyJob crawl exchange history quotes
func (s Scheduler) historyJob(wg *sync.WaitGroup, exchange exchanges.Exchange, start time.Time) {
	// fix start time
	start = utils.TodayZero(start.In(exchange.Location()))
	end := utils.YesterdayZero(time.Now().In(exchange.Location()))
	zap.L().Info("exchange history job start",
		zap.String("exchange", exchange.Code()),
		zap.Time("start", start),
		zap.Time("end", end))

	var dates []time.Time
	for date := start; !date.After(end); date = date.AddDate(0, 0, 1) {
		exists, err := s.store.Exists(exchange, date)
		if err != nil {
			zap.L().Error("check exchange daily quote exists failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Time("date", date))
		} else {
			if !exists {
				dates = append(dates, date)
			}
		}
	}

	err := s.crawl(exchange, dates...)
	if err != nil {
		err = utils.GetWeChatService().SendMessage(fmt.Sprintf("history job failed due to %s", err))
		if err != nil {
			zap.L().Fatal("send history job failed message failed", zap.Error(err))
		}
		zap.L().Debug("send history job failed message success")

		zap.L().Fatal("exchange history job failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Times("dates", dates))
	}

	zap.L().Info("exchange history job finished",
		zap.String("exchange", exchange.Code()),
		zap.Time("start", start),
		zap.Time("end", end))
}

// dailyJob crawl exchange daily qoutes
func (s Scheduler) dailyJob(wg *sync.WaitGroup, exchange exchanges.Exchange) {
	now := time.Now().In(exchange.Location())
	duration2Tomorrow := utils.TomorrowZero(now).Sub(now)
	zap.L().Info("exchange daily job start",
		zap.String("exchange", exchange.Code()),
		zap.Duration("in", duration2Tomorrow))

	var err error
	for {
		// wait for tomorrow zero clock
		beforeCrawl := <-time.After(duration2Tomorrow)
		yesterday := utils.YesterdayZero(beforeCrawl.In(exchange.Location()))
		zap.L().Info("exchange daily job start",
			zap.String("exchange", exchange.Code()),
			zap.Time("date", yesterday))

		for index := 0; index < constants.RetryCount; index++ {
			// crawl
			err = s.crawl(exchange, yesterday)
			if err != nil && index < constants.RetryCount-1 {
				zap.L().Warn("crawl exchange daily quote failed",
					zap.Error(err),
					zap.Duration("retry in", constants.RetryInterval),
					zap.String("retries", fmt.Sprintf("%d/%d", index+1, constants.RetryCount)))
				time.Sleep(constants.RetryInterval)
				continue
			}

			break
		}

		afterCrawl := time.Now().In(exchange.Location())
		duration2Tomorrow = utils.TomorrowZero(afterCrawl).Sub(afterCrawl)
		if err != nil {
			zap.L().Error("exchange daily job failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Time("date", yesterday),
				zap.Duration("to tomorrow", duration2Tomorrow))

			err = utils.GetWeChatService().SendMessage(fmt.Sprintf("daily job failed due to %s", err))
			if err != nil {
				zap.L().Fatal("send daily job failed message failed", zap.Error(err))
			}
			zap.L().Debug("send daily job failed message success")
		} else {
			zap.L().Info("exchange daily job success",
				zap.String("exchange", exchange.Code()),
				zap.Time("date", yesterday),
				zap.Duration("duration", afterCrawl.Sub(beforeCrawl)),
				zap.Duration("to tomorrow", duration2Tomorrow))
		}
	}
}

// crawl crawl exchange quotes in special days
func (s Scheduler) crawl(exchange exchanges.Exchange, dates ...time.Time) error {
	if len(dates) == 0 {
		return nil
	}

	zap.L().Debug("try to get exchange companies", zap.String("exchange", exchange.Code()))

	// get companies
	companies, err := exchange.Companies()
	if err != nil {
		zap.L().Error("get exchange companies failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()))
		return err
	}

	zap.L().Info("get exchange companies success",
		zap.String("exchange", exchange.Code()),
		zap.Int("companies", len(companies)))

	for _, date := range dates {
		result := &notifiers.ExchangeDailyJobResult{
			Exchange: exchange.Code(),
			Date:     date.Unix(),
			Success:  true,
		}

		err = s.crawlOneDay(exchange, companies, date)
		if err != nil {
			zap.L().Error("crawl exchange companies failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Time("date", date))

			result.Success = false
			// s.notifier.Notify(result)

			return err
		}

		// s.notifier.Notify(result)
	}

	return nil
}

// crawlOneDay crawl exchange quotes in special day
func (s Scheduler) crawlOneDay(exchange exchanges.Exchange, companies map[string]*quotes.Company, date time.Time) error {
	// crawl
	cdqs, err := s.crawlCompaniesDailyQuote(exchange, companies, date)
	if err != nil {
		zap.L().Error("get exchange company quotes failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	// make empty companies map if is not trading day
	if len(cdqs) == 0 {
		companies = make(map[string]*quotes.Company)
	}

	edq := &quotes.ExchangeDailyQuote{
		Exchange:  exchange.Code(),
		Date:      date,
		Companies: companies,
		Quotes:    cdqs,
	}

	// save
	err = s.store.Save(exchange, date, edq)
	if err != nil {
		zap.L().Error("save exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	zap.L().Info("save exchange daily quote success",
		zap.Error(err),
		zap.String("exchange", exchange.Code()),
		zap.Time("date", date),
		zap.Int("total companies", len(companies)),
		zap.Int("valid companies", len(cdqs)))

	return nil
}

// crawlCompaniesDailyQuote crawl company quotes in special day
func (s Scheduler) crawlCompaniesDailyQuote(exchange exchanges.Exchange, companies map[string]*quotes.Company, date time.Time) (map[string]*quotes.CompanyDailyQuote, error) {
	wg := new(sync.WaitGroup)
	wg.Add(len(companies))

	zap.S().Infow("companies daili", "exchange", exchange.Code(), "companies", len(companies), "date", date.Format("20060102"))
	mutex := new(sync.Mutex)
	cdqs := make(map[string]*quotes.CompanyDailyQuote, len(companies))
	for _, company := range companies {
		go func(_company *quotes.Company) {
			cdq, err := exchange.Crawl(_company, date)
			if err == nil && !cdq.IsEmpty() {
				mutex.Lock()
				cdqs[_company.Code] = cdq
				// if len(cdqs)%10 == 0 {
				// 	zap.S().Infow("continue", "companies", len(cdqs), "total", len(companies))
				// }
				mutex.Unlock()
			}

			s.limiter.Release()
			wg.Done()
		}(company)

		s.limiter.Set()
	}
	wg.Wait()

	return cdqs, nil
}
