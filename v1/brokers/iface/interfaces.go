package iface

import (
	"context"
	"github.com/gomodule/redigo/redis"
	"github.com/piaobeizu/machinery/v1/monitor"

	"github.com/piaobeizu/machinery/v1/config"
	"github.com/piaobeizu/machinery/v1/tasks"
)

type ConsumeFunc func(msg redis.Message) error

// Broker - a common interface for all brokers
type Broker interface {
	GetConfig() *config.Config
	SetRegisteredTaskNames(names []string)
	IsTaskRegistered(name string) bool
	StartConsuming(consumerTag string, concurrency int, p TaskProcessor) (bool, error)
	StopConsuming()
	SendHeartbeat(heartbeat *monitor.Heartbeat, queue string) error
	ConsumeHeartbeat(ctx context.Context, queue string, consume ConsumeFunc) (*monitor.Heartbeat, error)

	Publish(ctx context.Context, task *tasks.Signature) error
	GetPendingTasks(queue string) ([]*tasks.Signature, error)
	AdjustRoutingKey(s *tasks.Signature)
	// cycle signatures monitor
	GetCycleTasks(queue string) ([]*tasks.Signature, error)
	AddCycleTask(signature *tasks.Signature) (*tasks.Signature, error)
	DeleteCycleTask(uuid string) error
}

// TaskProcessor - can process a delivered task
// This will probably always be a worker instance
type TaskProcessor interface {
	Process(signature *tasks.Signature) error
	CustomQueue() string
}
