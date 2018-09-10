package constants

import "time"

const (
	// RetryCount defind retry count
	RetryCount = 6
	// RetryInterval define retry intervals
	RetryInterval = time.Second * 10
	// DefaultParallel define default parallel
	DefaultParallel = 64
	// DatePattern define date compact pattern
	DatePattern = "20060102"
)