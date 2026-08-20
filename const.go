package redisqueue

import "time"

var (
	// PoolReleaseTimeout 关闭协程池时,等待处理中消息完成的最大时长
	PoolReleaseTimeout = 30 * time.Second
)
