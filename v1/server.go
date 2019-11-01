package machinery

import (
	"context"
	"errors"
	"fmt"
	"github.com/piaobeizu/cron"
	"github.com/piaobeizu/machinery/v1/log"
	"github.com/piaobeizu/machinery/v1/monitor"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/piaobeizu/machinery/v1/backends/result"
	"github.com/piaobeizu/machinery/v1/brokers/eager"
	"github.com/piaobeizu/machinery/v1/config"
	"github.com/piaobeizu/machinery/v1/tasks"
	"github.com/piaobeizu/machinery/v1/tracing"

	opentracing "github.com/opentracing/opentracing-go"
	backendsiface "github.com/piaobeizu/machinery/v1/backends/iface"
	brokersiface "github.com/piaobeizu/machinery/v1/brokers/iface"
)

// Server is the main Machinery object and stores all configuration
// All the tasks workers process are registered against the server
type Server struct {
	config            *config.Config
	monitor           *Monitor
	registeredTasks   map[string]interface{}
	cycleSignatures   map[string]*tasks.Signature //周期调度的signature
	broker            brokersiface.Broker
	backend           backendsiface.Backend
	prePublishHandler func(*tasks.Signature)
	lock              sync.RWMutex
}

// NewServerWithBrokerBackend ...
func NewServerWithBrokerBackend(cnf *config.Config, brokerServer brokersiface.Broker, backendServer backendsiface.Backend) *Server {
	return &Server{
		config: cnf,
		monitor: &Monitor{
			MType:  monitor.MACHINERY_SERVER,
			Broker: brokerServer,
		},
		registeredTasks: make(map[string]interface{}),
		broker:          brokerServer,
		backend:         backendServer,
		cycleSignatures: make(map[string]*tasks.Signature),
	}
}

// NewServer creates Server instance
func NewServer(cnf *config.Config) (*Server, error) {
	broker, err := BrokerFactory(cnf)
	if err != nil {
		return nil, err
	}

	// Backend is optional so we ignore the error
	backend, _ := BackendFactory(cnf)

	srv := NewServerWithBrokerBackend(cnf, broker, backend)

	// init for eager-mode
	eager, ok := broker.(eager.Mode)
	if ok {
		// we don't have to call worker.Launch in eager mode
		eager.AssignWorker(srv.NewWorker("eager", "eager", 0, monitor.OTHER_CLUSTER))
	}

	return srv, nil
}

// NewWorker creates Worker instance
func (server *Server) NewWorker(workerName string, consumerTag string, concurrency int, mType monitor.MACHINE_TYPE) *Worker {
	return &Worker{
		server: server,
		Monitor: &Monitor{
			MType:      mType,
			Broker:     server.broker,
			WorkerName: workerName,
		},
		ConsumerTag: consumerTag,
		Concurrency: concurrency,
		Queue:       string(mType),
	}
}

func (server *Server) ServerMonitor() error {
	// 启动cycle任务监控
	if err := cycleTaskMonitor(server); err != nil {
		return err
	}
	// 启动server监控
	if err := server.monitor.ServerMonitor(); err != nil {
		return err
	}
	return nil
}

// NewCustomQueueWorker creates Worker instance with Custom Queue
func (server *Server) NewCustomQueueWorker(consumerTag string, concurrency int, queue string) *Worker {
	return &Worker{
		server:      server,
		ConsumerTag: consumerTag,
		Concurrency: concurrency,
		Queue:       queue,
	}
}

// GetBroker returns broker
func (server *Server) GetBroker() brokersiface.Broker {
	return server.broker
}

// SetBroker sets broker
func (server *Server) SetBroker(broker brokersiface.Broker) {
	server.broker = broker
}

// GetBackend returns backend
func (server *Server) GetBackend() backendsiface.Backend {
	return server.backend
}

// SetBackend sets backend
func (server *Server) SetBackend(backend backendsiface.Backend) {
	server.backend = backend
}

// GetConfig returns connection object
func (server *Server) GetConfig() *config.Config {
	return server.config
}

// SetConfig sets config
func (server *Server) SetConfig(cnf *config.Config) {
	server.config = cnf
}

// SetPreTaskHandler Sets pre publish handler
func (server *Server) SetPreTaskHandler(handler func(*tasks.Signature)) {
	server.prePublishHandler = handler
}

// RegisterTasks registers all tasks at once
func (server *Server) RegisterTasks(namedTaskFuncs map[string]interface{}) error {
	for _, task := range namedTaskFuncs {
		if err := tasks.ValidateTask(task); err != nil {
			return err
		}
	}
	server.registeredTasks = namedTaskFuncs
	server.broker.SetRegisteredTaskNames(server.GetRegisteredTaskNames())
	return nil
}

// RegisterTask registers a single task
func (server *Server) RegisterTask(name string, taskFunc interface{}) error {
	if err := tasks.ValidateTask(taskFunc); err != nil {
		return err
	}
	server.registeredTasks[name] = taskFunc
	server.broker.SetRegisteredTaskNames(server.GetRegisteredTaskNames())
	return nil
}

// IsTaskRegistered returns true if the task name is registered with this broker
func (server *Server) IsTaskRegistered(name string) bool {
	_, ok := server.registeredTasks[name]
	return ok
}

// GetRegisteredTask returns registered task by name
func (server *Server) GetRegisteredTask(name string) (interface{}, error) {
	taskFunc, ok := server.registeredTasks[name]
	if !ok {
		return nil, fmt.Errorf("Task not registered error: %s", name)
	}
	return taskFunc, nil
}

// SendTaskWithContext will inject the trace context in the signature headers before publishing it
func (server *Server) SendTaskWithContext(ctx context.Context, signature *tasks.Signature) (*result.AsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendTask", tracing.ProducerOption(), tracing.MachineryTag)
	defer span.Finish()

	// tag the span with some info about the signature
	signature.Headers = tracing.HeadersWithSpan(signature.Headers, span)

	// Make sure result backend is defined
	if server.backend == nil {
		return nil, errors.New("Result backend required")
	}

	// Auto generate a UUID if not set already
	if signature.UUID == "" {
		taskID := uuid.New().String()
		signature.UUID = fmt.Sprintf("task_%v", taskID)
	}

	// Set initial task state to PENDING
	if err := server.backend.SetStatePending(signature); err != nil {
		return nil, fmt.Errorf("Set state pending error: %s", err)
	}

	if server.prePublishHandler != nil {
		server.prePublishHandler(signature)
	}

	if err := server.broker.Publish(ctx, signature); err != nil {
		return nil, fmt.Errorf("Publish message error: %s", err)
	}

	return result.NewAsyncResult(signature, server.backend), nil
}

// SendTask publishes a task to the default queue
func (server *Server) SendTask(signature *tasks.Signature) (*result.AsyncResult, error) {
	return server.SendTaskWithContext(context.Background(), signature)
}

// SendTaskWithContext will inject the trace context in the signature headers before publishing it
func (server *Server) SendCycleWithContext(ctx context.Context, signature *tasks.Signature) (*result.AsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendTask", tracing.ProducerOption(), tracing.MachineryTag)
	defer span.Finish()

	// tag the span with some info about the signature
	signature.Headers = tracing.HeadersWithSpan(signature.Headers, span)

	// Make sure result backend is defined
	if server.backend == nil {
		return nil, errors.New("Result backend required")
	}

	// Auto generate a UUID if not set already
	if signature.UUID == "" {
		taskID := uuid.New().String()
		signature.UUID = fmt.Sprintf("task_%v", taskID)
	}

	// Set initial task state to PENDING
	if err := server.backend.SetStatePending(signature); err != nil {
		return nil, fmt.Errorf("Set state pending error: %s", err)
	}

	if server.prePublishHandler != nil {
		server.prePublishHandler(signature)
	}
	// 注册cycle任务到broker
	_, err := server.broker.AddCycleTask(signature)
	if err != nil {
		return nil, err
	}
	// 添加signature到cycleSignatures
	server.cycleSignatures[signature.UUID] = signature
	if err = addCycleToCron(signature, server); err != nil {
		return nil, err
	}
	return result.NewAsyncResult(signature, server.backend), nil
}

// SendTask publishes a task to the default queue
func (server *Server) SendCycle(signature *tasks.Signature) (*result.AsyncResult, error) {
	return server.SendCycleWithContext(context.Background(), signature)
}

// SendChainWithContext will inject the trace context in all the signature headers before publishing it
func (server *Server) SendChainWithContext(ctx context.Context, chain *tasks.Chain) (*result.ChainAsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendChain", tracing.ProducerOption(), tracing.MachineryTag, tracing.WorkflowChainTag)
	defer span.Finish()

	tracing.AnnotateSpanWithChainInfo(span, chain)

	return server.SendChain(chain)
}

// SendChain triggers a chain of tasks
func (server *Server) SendChain(chain *tasks.Chain) (*result.ChainAsyncResult, error) {
	_, err := server.SendTask(chain.Tasks[0])
	if err != nil {
		return nil, err
	}

	return result.NewChainAsyncResult(chain.Tasks, server.backend), nil
}

// SendGroupWithContext will inject the trace context in all the signature headers before publishing it
func (server *Server) SendGroupWithContext(ctx context.Context, group *tasks.Group, sendConcurrency int) ([]*result.AsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendGroup", tracing.ProducerOption(), tracing.MachineryTag, tracing.WorkflowGroupTag)
	defer span.Finish()

	tracing.AnnotateSpanWithGroupInfo(span, group, sendConcurrency)

	// Make sure result backend is defined
	if server.backend == nil {
		return nil, errors.New("Result backend required")
	}

	asyncResults := make([]*result.AsyncResult, len(group.Tasks))

	var wg sync.WaitGroup
	wg.Add(len(group.Tasks))
	errorsChan := make(chan error, len(group.Tasks)*2)

	// Init group
	server.backend.InitGroup(group.GroupUUID, group.GetUUIDs())

	// Init the tasks Pending state first
	for _, signature := range group.Tasks {
		if err := server.backend.SetStatePending(signature); err != nil {
			errorsChan <- err
			continue
		}
	}

	pool := make(chan struct{}, sendConcurrency)
	go func() {
		for i := 0; i < sendConcurrency; i++ {
			pool <- struct{}{}
		}
	}()

	for i, signature := range group.Tasks {

		if sendConcurrency > 0 {
			<-pool
		}

		go func(s *tasks.Signature, index int) {
			defer wg.Done()

			// Publish task

			err := server.broker.Publish(ctx, s)

			if sendConcurrency > 0 {
				pool <- struct{}{}
			}

			if err != nil {
				errorsChan <- fmt.Errorf("Publish message error: %s", err)
				return
			}

			asyncResults[index] = result.NewAsyncResult(s, server.backend)
		}(signature, i)
	}

	done := make(chan int)
	go func() {
		wg.Wait()
		done <- 1
	}()

	select {
	case err := <-errorsChan:
		return asyncResults, err
	case <-done:
		return asyncResults, nil
	}
}

// SendGroup triggers a group of parallel tasks
func (server *Server) SendGroup(group *tasks.Group, sendConcurrency int) ([]*result.AsyncResult, error) {
	return server.SendGroupWithContext(context.Background(), group, sendConcurrency)
}

// SendChordWithContext will inject the trace context in all the signature headers before publishing it
func (server *Server) SendChordWithContext(ctx context.Context, chord *tasks.Chord, sendConcurrency int) (*result.ChordAsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendChord", tracing.ProducerOption(), tracing.MachineryTag, tracing.WorkflowChordTag)
	defer span.Finish()

	tracing.AnnotateSpanWithChordInfo(span, chord, sendConcurrency)

	_, err := server.SendGroupWithContext(ctx, chord.Group, sendConcurrency)
	if err != nil {
		return nil, err
	}

	return result.NewChordAsyncResult(
		chord.Group.Tasks,
		chord.Callback,
		server.backend,
	), nil
}

// SendChord triggers a group of parallel tasks with a callback
func (server *Server) SendChord(chord *tasks.Chord, sendConcurrency int) (*result.ChordAsyncResult, error) {
	return server.SendChordWithContext(context.Background(), chord, sendConcurrency)
}

// GetRegisteredTaskNames returns slice of registered task names
func (server *Server) GetRegisteredTaskNames() []string {
	taskNames := make([]string, len(server.registeredTasks))
	var i = 0
	for name := range server.registeredTasks {
		taskNames[i] = name
		i++
	}
	return taskNames
}

func (server *Server) GetCycleTasks() map[string]*tasks.Signature {
	return server.cycleSignatures
}

func cycleTaskMonitor(server *Server) error {
	// 获取redis存储的cycle signature
	signatures, err := server.broker.GetCycleTasks("")
	if err != nil {
		return err
	}
	for _, signature := range signatures {
		if _, ok := server.cycleSignatures[signature.UUID]; !ok {
			server.setCycleSignatures(signature)
		}
		if signature.EndTime > time.Now().Unix() {
			if err = addCycleToCron(signature, server); err != nil {
				log.ERROR.Println("add cron task [%v] error, %v", signature, err)
				return err
			}
		}
	}
	// 定时每隔1分钟清理到期的cycle signature
	c := cron.New(cron.WithSeconds())
	//AddFunc 函数中包含了对cron表达式的校验
	_, err = c.AddFunc("* */1 * * * ?", func() {
		for key, sig := range server.cycleSignatures {
			if sig.EndTime < time.Now().Unix() {
				if err = server.broker.DeleteCycleTask(key); err != nil {
					log.INFO.Printf("delete cycle task [%v] error, %v", sig, err)
				}
				server.delCycleSignatures(key)
				log.INFO.Printf("delete cycle task which is ended, %v", sig)
			}
		}
	})
	c.Start()
	log.INFO.Println("start monitor cycle task")
	if err != nil {
		log.ERROR.Println("add cron to delete end task error, %v", err)
		return nil
	}
	return nil
}

func (server *Server) setCycleSignatures(signature *tasks.Signature) {
	server.lock.Lock()
	server.cycleSignatures[signature.UUID] = signature
	server.lock.Unlock()
}

func (server *Server) delCycleSignatures(key string) {
	server.lock.Lock()
	delete(server.cycleSignatures, key)
	server.lock.Unlock()
}

func addCycleToCron(signature *tasks.Signature, server *Server) error {
	if signature.CronRule != "" {
		var (
			c   *cron.Cron
			err error
		)
		if signature.StartTime > 0 && signature.EndTime > 0 {
			c, err = cron.DelayNew(signature.StartTime, signature.EndTime, cron.WithSeconds())
			if err != nil {
				log.ERROR.Printf("create cron task error, cause is: %s", err)
				return fmt.Errorf("create cron task error, cause is: %s", err)
			}
		} else {
			c = cron.New(cron.WithSeconds())
		}
		//AddFunc 函数中包含了对cron表达式的校验
		_, err = c.AddFunc(signature.CronRule, func() {
			log.INFO.Printf("start publish message")
			if err := server.broker.Publish(context.Background(), signature); err != nil {
				log.ERROR.Printf("Publish message error: %s", err)
			}
		})
		if err != nil {
			log.ERROR.Printf("add cron task error: %s", err)
			return err
		}
		c.Start()
	} else {
		if err := server.broker.Publish(context.Background(), signature); err != nil {
			log.ERROR.Printf("Publish message error: %s", err)
			return nil
		}
	}
	return nil
}
