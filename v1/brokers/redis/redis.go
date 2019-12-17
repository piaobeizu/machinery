package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/piaobeizu/machinery/v1/monitor"
	"sync"
	"time"

	"github.com/RichardKnop/redsync"
	"github.com/gomodule/redigo/redis"
	"github.com/piaobeizu/machinery/v1/brokers/errs"
	"github.com/piaobeizu/machinery/v1/brokers/iface"
	"github.com/piaobeizu/machinery/v1/common"
	"github.com/piaobeizu/machinery/v1/config"
	"github.com/piaobeizu/machinery/v1/log"
	"github.com/piaobeizu/machinery/v1/tasks"
)

var redisDelayedTasksKey = "delayed_tasks"

// Broker represents a Redis broker
type Broker struct {
	common.Broker
	common.RedisConnector
	host         string
	password     string
	db           int
	pool         *redis.Pool
	consumingWG  sync.WaitGroup // wait group to make sure whole consumption completes
	processingWG sync.WaitGroup // use wait group to make sure task processing completes
	delayedWG    sync.WaitGroup
	// If set, path to a socket file overrides hostname
	socketPath string
	redsync    *redsync.Redsync
	redisOnce  sync.Once
}

// New creates new Broker instance
func New(cnf *config.Config, host, password, socketPath string, db int) iface.Broker {
	b := &Broker{Broker: common.NewBroker(cnf)}
	b.host = host
	b.db = db
	b.password = password
	b.socketPath = socketPath

	return b
}

// StartConsuming enters a loop and waits for incoming messages
func (b *Broker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (bool, error) {
	b.consumingWG.Add(1)
	defer b.consumingWG.Done()

	if concurrency < 1 {
		concurrency = 1
	}

	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)

	conn := b.open()
	defer conn.Close()

	// Ping the server to make sure connection is live
	_, err := conn.Do("PING")
	if err != nil {
		b.GetRetryFunc()(b.GetRetryStopChan())

		// Return err if retry is still true.
		// If retry is false, broker.StopConsuming() has been called and
		// therefore Redis might have been stopped. Return nil exit
		// StartConsuming()
		if b.GetRetry() {
			return b.GetRetry(), err
		}
		return b.GetRetry(), errs.ErrConsumerStopped
	}

	// Channel to which we will push tasks ready for processing by worker
	deliveries := make(chan []byte, concurrency)
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}

	// A receiving goroutine keeps popping messages from the queue by BLPOP
	// If the message is valid and can be unmarshaled into a proper structure
	// we send it to the deliveries channel
	go func() {

		log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				close(deliveries)
				return
			case <-pool:
				task, _ := b.nextTask(getQueue(b.GetConfig(), taskProcessor))
				//TODO: should this error be ignored?
				if len(task) > 0 {
					deliveries <- task
				}

				pool <- struct{}{}
			}
		}
	}()

	// A goroutine to watch for delayed tasks and push them to deliveries
	// channel for consumption by the worker
	b.delayedWG.Add(1)
	go func() {
		defer b.delayedWG.Done()

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				return
			default:
				task, err := b.nextDelayedTask(redisDelayedTasksKey)
				if err != nil {
					continue
				}

				signature := new(tasks.Signature)
				decoder := json.NewDecoder(bytes.NewReader(task))
				decoder.UseNumber()
				if err := decoder.Decode(signature); err != nil {
					log.ERROR.Print(errs.NewErrCouldNotUnmarshaTaskSignature(task, err))
				}

				if err := b.Publish(context.Background(), signature); err != nil {
					log.ERROR.Print(err)
				}
			}
		}
	}()

	if err := b.consume(deliveries, concurrency, taskProcessor); err != nil {
		return b.GetRetry(), err
	}

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()

	return b.GetRetry(), nil
}

// StartConsuming enters a loop and waits for incoming messages
func (b *Broker) SendHeartbeat(heartbeat *monitor.Heartbeat, queue string) error {
	msg, err := json.Marshal(heartbeat)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}
	conn := b.open()
	defer conn.Close()
	if queue == b.GetConfig().MonitorWorkerQueue {
		_, err = conn.Do("RPUSH", queue, msg)
	} else {
		_, err = redis.Int(conn.Do("PUBLISH", queue, msg))
		if err != nil {
			log.FATAL.Fatalf("redis publish %s %s, err: %v", queue, msg, err)
		}
	}
	return err
}

// StartConsuming enters a loop and waits for incoming messages
func (b *Broker) ConsumeHeartbeat(ctx context.Context, queue string, consume iface.ConsumeFunc) (*monitor.Heartbeat, error) {
	pollPeriod := time.Duration(1000) * time.Millisecond
	conn := b.open()
	defer conn.Close()
	if queue == b.GetConfig().MonitorWorkerQueue {
		items, err := redis.ByteSlices(conn.Do("BLPOP", queue, pollPeriod.Seconds()))
		if err != nil {
			return nil, err
		}
		if len(items) != 2 {
			return nil, redis.ErrNil
		}
		heartbeat := new(monitor.Heartbeat)
		decoder := json.NewDecoder(bytes.NewReader(items[1]))
		decoder.UseNumber()
		if err := decoder.Decode(heartbeat); err != nil {
			log.ERROR.Print("decode heartbeat error %v", err)
			return nil, err
		}
		return heartbeat, nil
	} else {
		psc := redis.PubSubConn{Conn: conn}
		if err := psc.Subscribe(redis.Args{}.AddFlat(queue)...); err != nil {
			return nil, err
		}
		done := make(chan error, 1)
		// start a new goroutine to receive message
		go func() {
			defer psc.Close()
			for {
				switch msg := psc.Receive().(type) {
				case error:
					done <- fmt.Errorf("redis pubsub receive err: %v", msg)
					return
				case redis.Message:
					if err := consume(msg); err != nil {
						done <- err
						return
					}
				case redis.Subscription:
					if msg.Count == 0 {
						// all channels are unsubscribed
						done <- nil
						return
					}
				}
			}
		}()

		// health check
		tick := time.NewTicker(time.Minute)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				if err := psc.Unsubscribe(); err != nil {
					return nil, fmt.Errorf("redis pubsub unsubscribe err: %v", err)
				}
				return nil, nil
			case err := <-done:
				return nil, err
			case <-tick.C:
				if err := psc.Ping(""); err != nil {
					return nil, err
				}
			}
		}
	}
}

// StopConsuming quits the loop
func (b *Broker) StopConsuming() {
	b.Broker.StopConsuming()
	// Waiting for the delayed tasks goroutine to have stopped
	b.delayedWG.Wait()
	// Waiting for consumption to finish
	b.consumingWG.Wait()

	if b.pool != nil {
		b.pool.Close()
	}
}

// Publish places a new message on the default queue
func (b *Broker) Publish(ctx context.Context, signature *tasks.Signature) error {
	// Adjust routing key (this decides which queue the message will be published to)
	b.Broker.AdjustRoutingKey(signature)

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	if signature.ExecCount == 0 {
		signature.ExecCount = 1
	}
	conn := b.open()
	defer conn.Close()

	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	if signature.ETA != nil {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			score := signature.ETA.UnixNano()
			_, err = conn.Do("ZADD", redisDelayedTasksKey, score, msg)
			return err
		}
	}

	_, err = conn.Do("RPUSH", signature.RoutingKey, msg)
	return err
}

// GetPendingTasks returns a slice of task signatures waiting in the queue
func (b *Broker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	conn := b.open()
	defer conn.Close()

	if queue == "" {
		queue = b.GetConfig().DefaultQueue
	}
	dataBytes, err := conn.Do("LRANGE", queue, 0, -1)
	if err != nil {
		return nil, err
	}
	results, err := redis.ByteSlices(dataBytes, err)
	if err != nil {
		return nil, err
	}

	taskSignatures := make([]*tasks.Signature, len(results))
	for i, result := range results {
		signature := new(tasks.Signature)
		decoder := json.NewDecoder(bytes.NewReader(result))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return nil, err
		}
		taskSignatures[i] = signature
	}
	return taskSignatures, nil
}

// get cycle signatures
func (b *Broker) GetCycleTasks(queue string) ([]*tasks.Signature, error) {
	conn := b.open()
	defer conn.Close()

	if queue == "" {
		queue = b.GetConfig().CycleQueue
	}
	dataBytes, err := conn.Do("HGETALL", queue)
	if err != nil {
		return nil, err
	}
	results, err := redis.ByteSlices(dataBytes, err)
	if err != nil {
		return nil, err
	}
	taskSignatures := make([]*tasks.Signature, len(results)/2)
	for i := 0; i < len(results); i = i + 2 {
		signature := new(tasks.Signature)
		decoder := json.NewDecoder(bytes.NewReader(results[i+1]))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return nil, err
		}
		// 将signature放入
		taskSignatures[i/2] = signature
	}
	return taskSignatures, nil
}

// get cycle signatures
func (b *Broker) AddCycleTask(signature *tasks.Signature) (*tasks.Signature, error) {
	conn := b.open()
	defer conn.Close()

	queue := b.GetConfig().CycleQueue

	msg, err := json.Marshal(signature)
	if err != nil {
		return nil, fmt.Errorf("JSON marshal error: %s", err)
	}

	_, err = conn.Do("HSET", queue, signature.UUID, msg)
	if err != nil {
		return nil, err
	}
	return signature, nil
}

func (b *Broker) DeleteCycleTask(uuid string) error {
	conn := b.open()
	defer conn.Close()

	queue := b.GetConfig().CycleQueue

	_, err := conn.Do("HDEL", queue, uuid)
	if err != nil {
		return err
	}
	return nil
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *Broker) consume(deliveries <-chan []byte, concurrency int, taskProcessor iface.TaskProcessor) error {
	errorsChan := make(chan error, concurrency*2)
	pool := make(chan struct{}, concurrency)

	// init pool for Worker tasks execution, as many slots as Worker concurrency param
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	for {
		select {
		case err := <-errorsChan:
			return err
		case d, open := <-deliveries:
			if !open {
				return nil
			}
			if concurrency > 0 {
				// get execution slot from pool (blocks until one is available)
				<-pool
			}

			b.processingWG.Add(1)

			// Consume the task inside a goroutine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d, taskProcessor); err != nil {
					errorsChan <- err
				}

				b.processingWG.Done()

				if concurrency > 0 {
					// give slot back to pool
					pool <- struct{}{}
				}
			}()
		}
	}
}

// consumeOne processes a single message using TaskProcessor
func (b *Broker) consumeOne(delivery []byte, taskProcessor iface.TaskProcessor) error {
	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(delivery))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return errs.NewErrCouldNotUnmarshaTaskSignature(delivery, err)
	}

	// If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(signature.Name) {
		if signature.IgnoreWhenTaskNotRegistered {
			return nil
		}
		log.INFO.Printf("Task not registered with this worker. Requeing message: %s", delivery)

		conn := b.open()
		defer conn.Close()

		conn.Do("RPUSH", getQueue(b.GetConfig(), taskProcessor), delivery)
		return nil
	}

	log.DEBUG.Printf("Received new message: %s", delivery)
	return taskProcessor.Process(signature)
}

// nextTask pops next available task from the default queue
func (b *Broker) nextTask(queue string) (result []byte, err error) {
	conn := b.open()
	defer conn.Close()

	pollPeriodMilliseconds := 1000 // default poll period for normal tasks
	if b.GetConfig().Redis != nil {
		configuredPollPeriod := b.GetConfig().Redis.NormalTasksPollPeriod
		if configuredPollPeriod > 0 {
			pollPeriodMilliseconds = configuredPollPeriod
		}
	}
	pollPeriod := time.Duration(pollPeriodMilliseconds) * time.Millisecond

	items, err := redis.ByteSlices(conn.Do("BLPOP", queue, pollPeriod.Seconds()))
	if err != nil {
		return []byte{}, err
	}

	// items[0] - the name of the key where an element was popped
	// items[1] - the value of the popped element
	if len(items) != 2 {
		return []byte{}, redis.ErrNil
	}

	result = items[1]

	return result, nil
}

// nextDelayedTask pops a value from the ZSET key using WATCH/MULTI/EXEC commands.
// https://github.com/gomodule/redigo/blob/master/redis/zpop_example_test.go
func (b *Broker) nextDelayedTask(key string) (result []byte, err error) {
	conn := b.open()
	defer conn.Close()

	defer func() {
		// Return connection to normal state on error.
		// https://redis.io/commands/discard
		if err != nil {
			conn.Do("DISCARD")
		}
	}()

	var (
		items [][]byte
		reply interface{}
	)

	pollPeriod := 500 // default poll period for delayed tasks
	if b.GetConfig().Redis != nil {
		configuredPollPeriod := b.GetConfig().Redis.DelayedTasksPollPeriod
		// the default period is 0, which bombards redis with requests, despite
		// our intention of doing the opposite
		if configuredPollPeriod > 0 {
			pollPeriod = configuredPollPeriod
		}
	}

	for {
		// Space out queries to ZSET so we don't bombard redis
		// server with relentless ZRANGEBYSCOREs
		time.Sleep(time.Duration(pollPeriod) * time.Millisecond)
		if _, err = conn.Do("WATCH", key); err != nil {
			return
		}

		now := time.Now().UTC().UnixNano()

		// https://redis.io/commands/zrangebyscore
		items, err = redis.ByteSlices(conn.Do(
			"ZRANGEBYSCORE",
			key,
			0,
			now,
			"LIMIT",
			0,
			1,
		))
		if err != nil {
			return
		}
		if len(items) != 1 {
			err = redis.ErrNil
			return
		}

		_ = conn.Send("MULTI")
		_ = conn.Send("ZREM", key, items[0])
		reply, err = conn.Do("EXEC")
		if err != nil {
			return
		}

		if reply != nil {
			result = items[0]
			break
		}
	}

	return
}

// open returns or creates instance of Redis connection
func (b *Broker) open() redis.Conn {
	b.redisOnce.Do(func() {
		b.pool = b.NewPool(b.socketPath, b.host, b.password, b.db, b.GetConfig().Redis, b.GetConfig().TLSConfig)
		b.redsync = redsync.New([]redsync.Pool{b.pool})
	})

	return b.pool.Get()
}

func getQueue(config *config.Config, taskProcessor iface.TaskProcessor) string {
	customQueue := taskProcessor.CustomQueue()
	if customQueue == "" {
		return config.DefaultQueue
	}
	return customQueue
}
