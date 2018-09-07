package schedulers

import (
	"sync"
	"time"

	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/stores"
	"github.com/nzai/qr/utils"
	"go.uber.org/zap"
)

// Scheduler define a crawl scheduler
type Scheduler struct {
	store     stores.Store
	exchanges []exchanges.Exchange
}

// NewScheduler create crawl scheduler
func NewScheduler(store stores.Store, exchanges ...exchanges.Exchange) *Scheduler {
	return &Scheduler{
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

// historyJob crawl exchange history quotes
func (s Scheduler) historyJob(wg *sync.WaitGroup, exchange exchanges.Exchange, start time.Time) {
	// fix start time
	start = utils.TodayZero(start.In(exchange.Location()))
	end := utils.TodayZero(time.Now().In(exchange.Location())).AddDate(0, 0, -1)
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

	for {
		// wait for tomorrow zero clock
		beforeCrawl := <-time.After(duration2Tomorrow)
		yesterday := utils.TodayZero(now.In(exchange.Location()))
		zap.L().Info("exchange daily job start",
			zap.String("exchange", exchange.Code()),
			zap.Time("date", yesterday))

		// crawl
		err := s.crawl(exchange, yesterday)

		afterCrawl := time.Now().In(exchange.Location())
		duration2Tomorrow = utils.TomorrowZero(afterCrawl).Sub(afterCrawl)
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

// crawl crawl exchange quotes in special days
func (s Scheduler) crawl(exchange exchanges.Exchange, dates ...time.Time) error {
	if len(dates) == 0 {
		return nil
	}

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
		err = s.crawlOneDay(exchange, companies, date)
		if err != nil {
			zap.L().Error("crawl exchange companies failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Time("date", date))
			return err
		}
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
	// limiter
	ch := make(chan bool, constants.DefaultParallel)
	defer close(ch)

	wg := new(sync.WaitGroup)
	wg.Add(len(companies))

	mutex := new(sync.Mutex)
	cdqs := make(map[string]*quotes.CompanyDailyQuote, len(companies))
	for _, company := range companies {
		go func(_company *quotes.Company) {
			cdq, err := exchange.Crawl(_company, date)
			// ignore error
			if err == nil && !cdq.IsEmpty() {
				mutex.Lock()
				cdqs[_company.Code] = cdq
				mutex.Unlock()
			}

			<-ch
			wg.Done()
		}(company)

		// limiter
		ch <- false
	}
	wg.Wait()

	return cdqs, nil
}
