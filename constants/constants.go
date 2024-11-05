package constants

import "time"

const (
	// RetryCount defind retry count
	RetryCount = 10
	// RetryInterval define retry intervals
	RetryInterval = time.Second * 30
	// DefaultParallel define default parallel
	DefaultParallel = 32
	// DatePattern define date compact pattern
	DatePattern = "20060102"
	// AwsSqsMaxBatchSize define max sqs batch size
	AwsSqsMaxBatchSize = 10
	// DefaultLastDays crawl last 20 days
	DefaultLastDays = 20
)
