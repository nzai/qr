package notifiers

// Notifier notify exchange daily job result
type Notifier interface {
	Notify(*ExchangeDailyJobResult)
	Close()
}
