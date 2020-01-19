package notifiers

// ExchangeDailyJobResult daily result
type ExchangeDailyJobResult struct {
	Exchange string `json:"exchange"`
	Date     int64  `json:"date"`
	Success  bool   `json:"success"`
}
