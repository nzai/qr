package constants

import "time"

const (
	// RetryCount 重试次数
	RetryCount = 6
	// RetryInterval 重试间隔
	RetryInterval = time.Second * 10
	// DefaultParallel 缺省并发数
	DefaultParallel = 64
)
