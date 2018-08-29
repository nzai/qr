package constants

import "time"

const (
	// RetryCount 重试次数
	RetryCount = 5
	// RetryInterval 重试间隔
	RetryInterval = time.Second * 10
	// DefaultParallel 缺省并发数
	DefaultParallel = 64
	// DatePattern 日期格式
	DatePattern = "2006-01-02"
)
