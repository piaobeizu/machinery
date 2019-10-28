package iface

import (
	"context"
	"github.com/piaobeizu/machinery/v1/monitor"

	"github.com/piaobeizu/machinery/v1/config"
	"github.com/piaobeizu/machinery/v1/tasks"
)

// Broker - a common interface for all brokers
type Broker interface {
	GetConfig() *config.Config
	SetRegisteredTaskNames(names []string)
	IsTaskRegistered(name string) bool
	StartConsuming(consumerTag string, concurrency int, p TaskProcessor) (bool, error)
	StopConsuming()
	SendHeartbeat(ctx context.Context, heartbeat *monitor.Heartbeat) error
	ConsumeHeartbeat() (*monitor.Heartbeat, error)

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
