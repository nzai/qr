package schedulers

import (
	"sync"
	"time"

	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/sources"
	"github.com/nzai/qr/stores"
	"go.uber.org/zap"
)

// Scheduler define a crawl scheduler
type Scheduler struct {
	source    sources.Source
	store     stores.Store
	exchanges []exchanges.Exchange
}

// NewScheduler create crawl scheduler
func NewScheduler(source sources.Source, store stores.Store, exchanges ...exchanges.Exchange) *Scheduler {

	return &Scheduler{
		source:    source,
		store:     store,
		exchanges: exchanges,
	}
}

// Run start jobs
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

// todayZero truncate time to today zero clock
func (s Scheduler) todayZero(now time.Time) time.Time {
	_, offset := now.Zone()
	return now.Truncate(time.Hour * 24).Add(-time.Second * time.Duration(offset))
}

// tomorrowZero round time to tomorrow zero clock
func (s Scheduler) tomorrowZero(now time.Time) time.Time {
	return s.todayZero(now).AddDate(0, 0, 1)
}

// historyJob crawl exchange history quotes
func (s Scheduler) historyJob(wg *sync.WaitGroup, exchange exchanges.Exchange, start time.Time) {

	yesterday := s.todayZero(time.Now().In(exchange.Location()))
	zap.L().Info("exchange history job start",
		zap.String("exchange", exchange.Code()),
		zap.Time("start", start),
		zap.Time("end", yesterday))

	var dates []time.Time
	for yesterday.After(start) {
		exists, err := s.store.Exists(exchange, start)
		if err != nil {
			zap.L().Error("check exchange daily quote exists failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Time("date", start))
		} else {
			if !exists {
				dates = append(dates, start)
			}
		}

		start = start.AddDate(0, 0, 1)
	}

	err := s.crawl(exchange, dates...)
	if err != nil {
		zap.L().Fatal("exchange history job failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Times("dates", dates))
	}

	zap.L().Info("exchange history job success",
		zap.String("exchange", exchange.Code()),
		zap.Time("start", start),
		zap.Time("end", yesterday))
}

// dailyJob crawl exchange daily qoutes
func (s Scheduler) dailyJob(wg *sync.WaitGroup, exchange exchanges.Exchange) {
	now := time.Now().In(exchange.Location())
	duration2Tomorrow := s.tomorrowZero(now).Sub(now)
	zap.L().Info("exchange daily job start",
		zap.String("exchange", exchange.Code()),
		zap.Duration("in", duration2Tomorrow))

	for {
		// wait for tomorrow zero clock
		beforeCrawl := <-time.After(duration2Tomorrow)
		yesterday := s.todayZero(now.In(exchange.Location()))
		zap.L().Info("exchange daily job start",
			zap.String("exchange", exchange.Code()),
			zap.Time("date", yesterday))

		// crawl
		err := s.crawl(exchange, yesterday)

		afterCrawl := time.Now().In(exchange.Location())
		duration2Tomorrow = s.tomorrowZero(afterCrawl).Sub(afterCrawl)
		if err != nil {
			zap.L().Error("exchange daily job failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Time("date", yesterday),
				zap.Duration("to tomorrow", duration2Tomorrow))
		} else {
			zap.L().Info("exchange daily job success",
				zap.String("exchange", exchange.Code()),
				zap.Time("date", yesterday),
				zap.Duration("duration", afterCrawl.Sub(beforeCrawl)),
				zap.Duration("to tomorrow", duration2Tomorrow))
		}
	}
}

// Record crawl exchange quotes in special days
func (s Scheduler) crawl(exchange exchanges.Exchange, dates ...time.Time) error {
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
		// crawl
		dailyQuotes, err := s.crawlCompaniesDailyQuote(exchange, companies, date)
		if err != nil {
			zap.L().Error("get exchange companies failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Time("date", date))
			return err
		}

		companyDict := make(map[string]*quotes.Company, len(companies))
		for _, company := range companies {
			companyDict[company.Code] = company
		}

		edq := &quotes.ExchangeDailyQuote{
			Version:   1,
			Exchange:  exchange.Code(),
			Date:      date,
			Companies: companyDict,
			Quotes:    dailyQuotes,
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
			zap.Time("date", date))
	}

	return nil
}

// crawl crawl company quotes in special day
func (s Scheduler) crawlCompaniesDailyQuote(exchange exchanges.Exchange, companies []*quotes.Company, date time.Time) (map[string]*quotes.DailyQuote, error) {

	ch := make(chan bool, constants.DefaultParallel)
	defer close(ch)

	wg := new(sync.WaitGroup)
	wg.Add(len(companies))

	mutex := new(sync.Mutex)
	dailyQuotes := make(map[string]*quotes.DailyQuote, len(companies))
	for _, company := range companies {
		go func(_company *quotes.Company) {
			dq, err := exchange.Crawl(_company, date)
			// ignore error
			if err == nil {
				mutex.Lock()
				dailyQuotes[_company.Code] = dq
				mutex.Unlock()
			}

			<-ch
			wg.Done()
		}(company)

		// limiter
		ch <- false
	}
	wg.Wait()

	return dailyQuotes, nil
}
