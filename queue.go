package redisqueue

import "time"

type StreamItem interface {
	// 队列名称
	GetQueue() string
	// 并发数
	GetConcurrency() int
	// 超时时间
	GetVisibilityTimeout() time.Duration
	// 缓冲区大小
	GetBufferSize() int
	// 是否禁用重试
	GetDisableRetry() bool
}
