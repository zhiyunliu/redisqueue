package redisqueue

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

// ConsumerFunc is a type alias for the functions that will be used to handle
// and process Messages.
type ConsumerFunc func(*Message) error

type registeredConsumer struct {
	stream            string
	fn                ConsumerFunc
	id                string
	visibilityTimeout time.Duration
	concurrency       int
	bufferSize        int
	disableRetry      bool

	consumer *Consumer
	// runnerPool 消息处理协程池,容量为 concurrency
	runnerPool    *ants.PoolWithFunc
	closePoolLock *sync.Once
}

// run 协程池的消息处理函数
func (item *registeredConsumer) run(arg interface{}) {
	msg, ok := arg.(*Message)
	if !ok {
		return
	}
	item.consumer.work(item, msg)
}

// releasePool 关闭协程池,等待正在处理中的消息处理完成
func (item *registeredConsumer) releasePool() {
	item.closePoolLock.Do(func() {
		_ = item.runnerPool.ReleaseTimeout(PoolReleaseTimeout)
	})
}

// freeCount 本次最多可以拉取的消息数量。协程池占满时至少拉取 1 条,拉取后会在
// 提交协程池时阻塞等待空闲协程,以此形成背压。
func (item *registeredConsumer) freeCount() int64 {
	cnt := item.bufferSize - item.runnerPool.Running()
	if cnt < 1 {
		cnt = 1
	}
	return int64(cnt)
}

// ConsumerOptions provide options to configure the Consumer.
type ConsumerOptions struct {
	// Name sets the name of this consumer. This will be used when fetching from
	// Redis. If empty, the hostname will be used.
	Name string
	// GroupName sets the name of the consumer group. This will be used when
	// coordinating in Redis. If empty, the hostname will be used.
	GroupName string
	// VisibilityTimeout dictates the maximum amount of time a message should
	// stay in pending. If there is a message that has been idle for more than
	// this duration, the consumer will attempt to claim it.
	VisibilityTimeout time.Duration
	// BlockingTimeout designates how long the XREADGROUP call blocks for. If
	// this is 0, it will block indefinitely. While this is the most efficient
	// from a polling perspective, if this call never times out, there is no
	// opportunity to yield back to Go at a regular interval. This means it's
	// possible that if no messages are coming in, the consumer cannot
	// gracefully shutdown. Instead, it's recommended to set this to 1-5
	// seconds, or even longer, depending on how long your application can wait
	// to shutdown.
	BlockingTimeout time.Duration
	// ReclaimInterval is the amount of time in between calls to XPENDING to
	// attempt to reclaim jobs that have been idle for more than the visibility
	// timeout. A smaller duration will result in more frequent checks. This
	// will allow messages to be reaped faster, but it will put more load on
	// Redis.
	ReclaimInterval time.Duration
	// BufferSize determines how many messages are fetched from Redis at a
	// time. This determines the maximum number of in-flight messages.
	BufferSize int
	// Concurrency dictates the size of the goroutine pool that handles the
	// messages, i.e. how many messages are processed at the same time.
	Concurrency int
	// RedisClient supersedes the RedisOptions field, and allows you to inject
	// an already-made Redis Client for use in the consumer. This may be either
	// the standard client or a cluster client.
	RedisClient redis.UniversalClient
	// RedisOptions allows you to configure the underlying Redis connection.
	// More info here:
	// https://pkg.go.dev/github.com/redis/go-redis/v9?tab=doc#Options.
	//
	// This field is used if RedisClient field is nil.
	RedisOptions *RedisOptions
}

// Consumer adds a convenient wrapper around dequeuing and managing concurrency.
type Consumer struct {
	// Errors is a channel that you can receive from to centrally handle any
	// errors that may occur either by your ConsumerFuncs or by internal
	// processing functions. Because this is an unbuffered channel, you must
	// have a listener on it. If you don't parts of the consumer could stop
	// functioning when errors occur due to the blocking nature of unbuffered
	// channels.
	Errors    chan error
	options   *ConsumerOptions
	redis     redis.UniversalClient
	consumers map[string]*registeredConsumer
	wg        *sync.WaitGroup
	closeChan chan struct{}
	closeOnce *sync.Once
	// enableXPendingExtIdele enable XPENDING EXTIDLE(支持redis 6.2.0以上版本)
	enableXPendingExtIdele bool
}

var defaultConsumerOptions = &ConsumerOptions{
	VisibilityTimeout: 60 * time.Second,
	BlockingTimeout:   5 * time.Second,
	ReclaimInterval:   1 * time.Second,
	BufferSize:        100,
	Concurrency:       10,
}

// NewConsumer uses a default set of options to create a Consumer. It sets Name
// to the hostname, GroupName to "redisqueue", VisibilityTimeout to 60 seconds,
// BufferSize to 100, and Concurrency to 10. In most production environments,
// you'll want to use NewConsumerWithOptions.
func NewConsumer() (*Consumer, error) {
	return NewConsumerWithOptions(defaultConsumerOptions)
}

// NewConsumerWithOptions creates a Consumer with custom ConsumerOptions. If
// Name is left empty, it defaults to the hostname; if GroupName is left empty,
// it defaults to "redisqueue"; if BlockingTimeout is 0, it defaults to 5
// seconds; if ReclaimInterval is 0, it defaults to 1 second.
func NewConsumerWithOptions(options *ConsumerOptions) (*Consumer, error) {
	hostname, _ := os.Hostname()

	if options.Name == "" {
		options.Name = hostname
	}
	if options.GroupName == "" {
		options.GroupName = "redisqueue"
	}
	if options.BlockingTimeout == 0 {
		options.BlockingTimeout = 5 * time.Second
	}
	if options.ReclaimInterval == 0 {
		options.ReclaimInterval = 1 * time.Second
	}

	var r redis.UniversalClient

	if options.RedisClient != nil {
		r = options.RedisClient
	} else {
		r = newRedisClient(options.RedisOptions)
	}

	if err := redisPreflightChecks(r); err != nil {
		return nil, err
	}

	enable, err := checkXPendingEnable(r)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		Errors:                 make(chan error),
		options:                options,
		redis:                  r,
		consumers:              make(map[string]*registeredConsumer),
		wg:                     &sync.WaitGroup{},
		closeChan:              make(chan struct{}),
		closeOnce:              &sync.Once{},
		enableXPendingExtIdele: enable,
	}, nil
}

// RegisterWithLastID is the same as Register, except that it also lets you
// specify the oldest message to receive when first creating the consumer group.
// This can be any valid message ID, "0" for all messages in the stream, or "$"
// for only new messages.
//
// If the consumer group already exists the id field is ignored, meaning you'll
// receive unprocessed messages.
func (c *Consumer) RegisterWithLastID(stream StreamItem, id string, fn ConsumerFunc) {
	if len(id) == 0 {
		id = "0"
	}

	concurrency := stream.GetConcurrency()
	if concurrency <= 0 {
		concurrency = c.options.Concurrency
	}
	visibilityTimeout := time.Duration(stream.GetVisibilityTimeout()) * time.Second
	if visibilityTimeout == 0 {
		visibilityTimeout = c.options.VisibilityTimeout
	}

	bufferSize := stream.GetBufferSize()
	if bufferSize == 0 {
		bufferSize = c.options.BufferSize
	}

	c.consumers[stream.GetQueue()] = &registeredConsumer{
		stream:            stream.GetQueue(),
		fn:                fn,
		id:                id,
		concurrency:       concurrency,
		visibilityTimeout: visibilityTimeout,
		bufferSize:        bufferSize,
		disableRetry:      stream.GetDisableRetry(),
		consumer:          c,
		closePoolLock:     &sync.Once{},
	}
}

// Register takes in a stream name and a ConsumerFunc that will be called when a
// message comes in from that stream. Register must be called at least once
// before Run is called. If the same stream name is passed in twice, the first
// ConsumerFunc is overwritten by the second.
func (c *Consumer) Register(stream StreamItem, fn ConsumerFunc) {
	c.RegisterWithLastID(stream, "", fn)
}

// Run starts all of the worker goroutines and starts processing from the
// streams that have been registered with Register. All errors will be sent to
// the Errors channel. If Register was never called, an error will be sent and
// Run will terminate early. The same will happen if an error occurs when
// creating the consumer group in Redis. Run will block until Shutdown is called
// and all of the in-flight messages have been processed.
func (c *Consumer) Run() {
	if len(c.consumers) == 0 {
		c.Errors <- errors.New("at least one consumer function needs to be registered")
		return
	}

	for stream, consumer := range c.consumers {
		//c.streams = append(c.streams, stream)
		err := c.redis.XGroupCreateMkStream(context.Background(), stream, c.options.GroupName, consumer.id).Err()
		// ignoring the BUSYGROUP error makes this a noop
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			c.releasePools()
			c.Errors <- errors.Wrapf(err, "error creating consumer group. stream[%s],group[%s]", stream, c.options.GroupName)
			return
		}

		//创建消息处理协程池,取代之前每个 stream 固定 concurrency 个常驻 goroutine 的方式
		consumer.runnerPool, err = ants.NewPoolWithFunc(consumer.concurrency, consumer.run)
		if err != nil {
			c.releasePools()
			c.Errors <- errors.Wrapf(err, "error creating goroutine pool. stream[%s],concurrency[%d]", stream, consumer.concurrency)
			return
		}
	}

	// for i := 0; i < len(c.consumers); i++ {
	// 	c.streams = append(c.streams, ">")
	// }

	go c.reclaim()
	c.poll()

	stop := newSignalHandler()
	go func() {
		<-stop
		c.Shutdown()
	}()

	c.wg.Wait()
}

// Shutdown stops new messages from being processed and tells the workers to
// wait until all in-flight messages have been processed, and then they exit.
// The order that things stop is 1) the reclaim process (if it's running), 2)
// the polling process, and 3) the goroutine pools. It is safe to call Shutdown
// more than once.
func (c *Consumer) Shutdown() {
	c.closeOnce.Do(func() {
		close(c.closeChan)
	})
}

// releasePools closes every registered goroutine pool, waiting up to
// PoolReleaseTimeout for the in-flight messages of each one to finish.
func (c *Consumer) releasePools() {
	for _, consumer := range c.consumers {
		if consumer.runnerPool == nil {
			continue
		}
		consumer.releasePool()
	}
}

// reclaim runs in a separate goroutine and checks the list of pending messages
// in every stream. For every message, if it's been idle for longer than the
// VisibilityTimeout, it will attempt to claim that message for this consumer.
// If VisibilityTimeout is 0, this function returns early and no messages are
// reclaimed. It checks the list of pending messages according to
// ReclaimInterval.
func (c *Consumer) reclaim() {
	if c.options.VisibilityTimeout == 0 {
		return
	}

	ticker := time.NewTicker(c.options.ReclaimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.closeChan:
			return
		case <-ticker.C:
			for _, cmgr := range c.consumers {
				c.reclaimQueue(cmgr)
			}
		}
	}
}

func (c *Consumer) reclaimQueue(cmgr *registeredConsumer) {

	//禁用重试
	if cmgr.disableRetry {
		return
	}

	var start = "-"
	var end = "+"

	pendingIdle := time.Duration(0)
	if c.enableXPendingExtIdele {
		pendingIdle = cmgr.visibilityTimeout
	}

	for {
		args := XPendingExtArgs{
			Stream: cmgr.stream,
			Group:  c.options.GroupName,
			Start:  start,
			End:    end,
			Idle:   pendingIdle, //空闲的时长
			Count:  cmgr.freeCount(),
		}
		redisArgs := redis.XPendingExtArgs(args)
		res, err := c.redis.XPendingExt(context.Background(), &redisArgs).Result()
		if err != nil {
			if strings.HasPrefix(err.Error(), "NOGROUP No such key") {
				break
			}
			if err != redis.Nil {
				c.Errors <- errors.Wrapf(err, "error listing pending messages. args[%s]", args)
				break
			}
		}

		if len(res) == 0 {
			break
		}

		msgs := make([]string, 0)

		for _, r := range res {
			if r.Idle < cmgr.visibilityTimeout {
				continue
			}

			args := XClaimArgs{
				Stream:   cmgr.stream,
				Group:    c.options.GroupName,
				Consumer: c.options.Name,
				MinIdle:  cmgr.visibilityTimeout,
				Messages: []string{r.ID},
			}
			redisArgs := redis.XClaimArgs(args)

			claimres, err := c.redis.XClaim(context.Background(), &redisArgs).Result()
			if err != nil && err != redis.Nil {
				c.Errors <- errors.Wrapf(err, "error claiming %d message(s)", len(msgs))
				break
			}
			// If the Redis nil error is returned, it means that
			// the message no longer exists in the stream.
			// However, it is still in a pending state. This
			// could happen if a message was claimed by a
			// consumer, that consumer died, and the message
			// gets deleted (either through a XDEL call or
			// through MAXLEN). Since the message no longer
			// exists, the only way we can get it out of the
			// pending state is to acknowledge it.
			if err == redis.Nil {
				err = c.redis.XAck(context.Background(), cmgr.stream, c.options.GroupName, r.ID).Err()
				if err != nil {
					c.Errors <- errors.Wrapf(err, "error acknowledging after failed claim. args[%s]", args)
					continue
				}
			}
			if len(claimres) > 0 {
				//lcmgr.
				c.enqueue(cmgr, claimres, r.RetryCount)
			}
		}

		newID, err := incrementMessageID(res[len(res)-1].ID)
		if err != nil {
			c.Errors <- err
			break
		}

		start = newID
	}

}

// poll constantly checks the streams using XREADGROUP to see if there are any
// messages for this consumer to process. It blocks for up to 5 seconds instead
// of blocking indefinitely so that it can periodically check to see if Shutdown
// was called.
func (c *Consumer) poll() {
	for k := range c.consumers {
		c.wg.Add(1)
		go func(item *registeredConsumer) {
			c.doReceive(item) //只有一次循环，不会造成内存重复copy
		}(c.consumers[k])
	}
}

func (c *Consumer) doReceive(consumer *registeredConsumer) {
	defer func() {
		//等待协程池中正在处理的消息处理完成
		consumer.releasePool()
		c.wg.Done()
	}()

	streams := []string{consumer.stream, ">"}

	readArgs := &redis.XReadGroupArgs{
		Group:    c.options.GroupName,
		Consumer: c.options.Name,
		Streams:  streams,
		Block:    c.options.BlockingTimeout,
	}

	for {
		select {
		case <-c.closeChan:
			return
		default:
			readArgs.Count = consumer.freeCount()
			res, err := c.redis.XReadGroup(context.Background(), readArgs).Result()
			if err != nil {
				if err, ok := err.(net.Error); ok && err.Timeout() {
					continue
				}
				if err == redis.Nil {
					continue
				}
				if strings.HasPrefix(err.Error(), "NOGROUP No such key") {
					time.Sleep(c.options.BlockingTimeout)
					continue
				}
				c.Errors <- errors.Wrap(err, fmt.Sprintf("error reading redis stream:args[%s]", XReadGroupArgs(*readArgs)))
				continue
			}

			for _, r := range res {
				c.enqueue(consumer, r.Messages, 0)
			}
		}
	}
}

// enqueue takes a slice of XMessages, creates corresponding Messages, and
// submits them to the stream's goroutine pool. Invoke blocks while the pool is
// saturated, which provides the same back pressure the buffered channel used to.
func (c *Consumer) enqueue(stream *registeredConsumer, msgs []redis.XMessage, retryCnt int64) {
	for _, m := range msgs {
		err := stream.runnerPool.Invoke(&Message{
			ID:         m.ID,
			RetryCount: retryCnt,
			Stream:     stream.stream,
			Values:     m.Values,
		})
		if err == nil {
			continue
		}
		//协程池已关闭,消息没有被ACK,重启后会由 reclaim 重新投递
		if err == ants.ErrPoolClosed || err == ants.ErrPoolOverload {
			return
		}
		c.Errors <- errors.Wrapf(err, "error dispatching to the pool for stream[%q] and message[%q]", stream.stream, m.ID)
	}
}

// work is called by the stream's goroutine pool. The number of messages handled
// at the same time is determined by Concurrency. It calls the corrensponding
// ConsumerFunc depending on the stream the message came from. If no error is
// returned from the ConsumerFunc, the message is acknowledged in Redis.
func (c *Consumer) work(stream *registeredConsumer, msg *Message) {
	err := c.process(stream, msg)
	if err != nil {
		return
	}
	err = c.redis.XAck(context.Background(), msg.Stream, c.options.GroupName, msg.ID).Err()
	if err != nil {
		c.Errors <- errors.Wrapf(err, "error acknowledging after success for stream[%q] and message[%q]", msg.Stream, msg.ID)
	}
}

func (c *Consumer) process(stream *registeredConsumer, msg *Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Consumer.process panic. stream[%s],message[%s],value:%+v error:%v", msg.Stream, msg.ID, msg.Values, r)
			c.Errors <- err
		}
	}()
	err = stream.fn(msg)
	return
}
