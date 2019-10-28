package machinery

import (
	"context"
	"github.com/piaobeizu/cron"
	"github.com/piaobeizu/machinery/v1/brokers/iface"
	"github.com/piaobeizu/machinery/v1/log"
	m "github.com/piaobeizu/machinery/v1/monitor"
)

type Monitor struct {
	MType      m.MACHINE_TYPE
	Broker     iface.Broker
	WorkerName string
}

var Machines = make(map[m.MACHINE_TYPE][]*m.Heartbeat)

// CustomQueue returns Custom Queue of the running worker process
func (monitor *Monitor) WorkerMonitor() error {
	// Log some useful information about worker configuration
	log.INFO.Printf("Launching a worker with the following settings:")
	log.INFO.Printf("- Broker: %s", monitor.Broker)
	c := cron.New(cron.WithSeconds())
	_, err := c.AddFunc("*/2 * * * * ?", func() {
		log.INFO.Printf("worker [%s] start publish a heartbeat to server", monitor.WorkerName)
		heartBeat, err := m.NewHeartBeat(monitor.WorkerName,monitor.MType)
		if err != nil {
			log.ERROR.Printf("create heartbeat error: %v", err)
		}
		//TODO heartbeat设置tasknum
		monitor.Broker.SendHeartbeat(context.Background(), heartBeat)
	})
	if err != nil {
		log.ERROR.Printf("add monitor error: %s", err)
		return err
	}
	c.Start()
	return nil
}

// CustomQueue returns Custom Queue of the running worker process
func (monitor *Monitor) ServerMonitor() error {
	// Log some useful information about worker configuration
	errorsChan := make(chan error)
	// Goroutine to start broker consumption and handle retries when broker connection dies
	go func() {
		for {
			heartbeat, err := monitor.Broker.ConsumeHeartbeat()
			if heartbeat != nil {
				_ = append(Machines[heartbeat.MachineType], heartbeat)
				log.INFO.Printf("receive a heartbeat %v", heartbeat)
				// TODO 收听到心跳后对worker做全面的分析诊断
			} else {
				errorsChan <- err // stop the goroutine
				return
			}
		}
	}()
	return nil
}
