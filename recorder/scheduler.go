package recorder

import (
	"sync"
	"time"

	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/sources"
	"github.com/nzai/qr/stores"
)

// Scheduler 调度器
type Scheduler struct {
	source    sources.Source
	exchanges []exchanges.Exchange
	recorders map[string]*Recorder
}

// NewScheduler 新建调度器
func NewScheduler(source sources.Source, store stores.Store, exchanges ...exchanges.Exchange) *Scheduler {
	recorders := make(map[string]*Recorder, len(exchanges))
	for _, exchange := range exchanges {
		recorders[exchange.Code()] = NewRecorder(exchange, store)
	}

	return &Scheduler{
		source:    source,
		exchanges: exchanges,
		recorders: recorders,
	}
}

// Run 运行
func (s Scheduler) Run(start time.Time) {
	if start.After(time.Now()) {
		return
	}

	wg := new(sync.WaitGroup)
	for _, exchange := range s.exchanges {
		wg.Add(2)

		// start history job
		go s.historyJob(wg, exchange, start)

		// start daily job
		go s.dailyJob(wg, exchange)
	}
	wg.Wait()
}

// historyJob 抓取历史数据任务
func (s Scheduler) historyJob(wg *sync.WaitGroup, exchange exchanges.Exchange, start time.Time) {

}

// dailyJob 抓取每日数据任务
func (s Scheduler) dailyJob(wg *sync.WaitGroup, exchange exchanges.Exchange) {
	now := time.Now().In(exchange.Location())
	tomorrow := now.Truncate(time.Hour*24).AddDate(0, 0, 1)
}
