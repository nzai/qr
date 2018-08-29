package recorder

import (
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/stores"
)

// Recorder 记录器
type Recorder struct {
	exchange exchanges.Exchange
	store    stores.Store // 存储
}

// NewRecorder 新建记录器
func NewRecorder(exchange exchanges.Exchange, store stores.Store) *Recorder {
	return &Recorder{
		exchange: exchange,
		store:    store,
	}
}

// Record 记录一天的报价
func (s Recorder) Record(date time.Time) error {
	// get companies
	companies, err := s.exchange.Companies()
	if err != nil {
		zap.L().Error("get exchange companies failed",
			zap.Error(err),
			zap.String("exchange", s.exchange.Code()),
			zap.Time("date", date))
		return err
	}

	zap.L().Info("get exchange companies success",
		zap.String("exchange", s.exchange.Code()),
		zap.Time("date", date),
		zap.Int("companies", len(companies)))

	// crawl
	dailyQuotes, err := s.crawl(companies, date)
	if err != nil {
		zap.L().Error("get exchange companies failed",
			zap.Error(err),
			zap.String("exchange", s.exchange.Code()),
			zap.Time("date", date))
		return err
	}

	companyDict := make(map[string]*quotes.Company, len(companies))
	for _, company := range companies {
		companyDict[company.Code] = company
	}

	edq := &quotes.ExchangeDailyQuote{
		Version:   1,
		Exchange:  s.exchange.Code(),
		Date:      date,
		Companies: companyDict,
		Quotes:    dailyQuotes,
	}

	// save
	err = s.store.Save(s.exchange, date, edq)
	if err != nil {
		zap.L().Error("save exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", s.exchange.Code()),
			zap.Time("date", date))
		return err
	}

	return nil
}

// crawl 抓取指定日期的市场报价
func (s Recorder) crawl(companies []*quotes.Company, date time.Time) (map[string]*quotes.DailyQuote, error) {

	ch := make(chan bool, constants.DefaultParallel)
	defer close(ch)

	wg := new(sync.WaitGroup)
	wg.Add(len(companies))

	mutex := new(sync.Mutex)
	dailyQuotes := make(map[string]*quotes.DailyQuote, len(companies))
	for _, company := range companies {
		go func(_company *quotes.Company) {
			dq, err := s.exchange.Crawl(_company, date)
			// ignore error
			if err == nil {
				mutex.Lock()
				dailyQuotes[_company.Code] = dq
				mutex.Unlock()
			}

			<-ch
			wg.Done()
		}(company)

		// 限流
		ch <- false
	}
	//	阻塞，直到抓取所有
	wg.Wait()

	return dailyQuotes, nil
}
