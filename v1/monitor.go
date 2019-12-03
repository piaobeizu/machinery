package machinery

import (
	"context"
	"fmt"
	"github.com/piaobeizu/cron"
	"github.com/piaobeizu/machinery/v1/brokers/iface"
	"github.com/piaobeizu/machinery/v1/log"
	m "github.com/piaobeizu/machinery/v1/monitor"
	"sync"
	"time"
)

type Monitor struct {
	MType      m.MACHINE_TYPE
	Broker     iface.Broker
	WorkerName string
}

var Machines = make(map[m.MACHINE_TYPE][]*m.Heartbeat)

//TODO server监控需要从逻辑上进行优化

// CustomQueue returns Custom Queue of the running worker process
func (monitor *Monitor) WorkerMonitor() error {
	serverAlive := serverAlive{Num: 1}
	// Log some useful information about worker configuration
	log.INFO.Printf("Launching a worker with the following settings:")
	log.INFO.Printf("- Broker: %s", monitor.Broker.GetConfig())
	c := cron.New(cron.WithSeconds())
	_, err := c.AddFunc("*/2 * * * * ?", func() {
		if (serverAlive.Num == 1) {
			log.INFO.Printf("worker [%s] start publish a heartbeat to server", monitor.WorkerName)
			heartBeat, err := m.NewHeartBeat(monitor.WorkerName, monitor.MType)
			if err != nil {
				log.ERROR.Printf("create heartbeat error: %v", err)
			}
			//TODO heartbeat设置tasknum
			monitor.Broker.SendHeartbeat(heartBeat, monitor.Broker.GetConfig().MonitorWorkerQueue)
		}
	})
	if err != nil {
		log.ERROR.Printf("add worker monitor error: %s", err)
		return err
	}
	c.Start()

	// 监控server心跳
	currentUnix := time.Now().Local().Unix()
	c2 := cron.New(cron.WithSeconds())
	_, err2 := c2.AddFunc("*/2 * * * * ?", func() {
		heartbeat, err2 := monitor.Broker.ConsumeHeartbeat(monitor.Broker.GetConfig().MonitorServerQueue)
		if err == nil || err2.Error() == "redigo: nil returned" {
			if heartbeat != nil {
				log.INFO.Printf("receive a server heartbeat %v", heartbeat)
				currentUnix = heartbeat.Timestamp
			}
		}
		serverAlive.Lock()
		if time.Now().Local().Unix()-currentUnix > 10 {
			serverAlive.Num = 0
			log.WARNING.Printf("there is no online server, please start it!!!")
		} else {
			serverAlive.Num = 1
		}
		serverAlive.Unlock()
	})
	if err2 != nil {
		log.ERROR.Printf("add server monitor error: %s", err2)
		return err2
	}
	c2.Start()
	return nil
}

// CustomQueue returns Custom Queue of the running worker process
func (monitor *Monitor) ServerMonitor() error {
	c := cron.New(cron.WithSeconds())
	_, err := c.AddFunc("*/2 * * * * ?", func() {
		log.INFO.Printf("server start publish a heartbeat to workers")
		heartBeat, err := m.NewHeartBeat("server", m.MACHINERY_SERVER)
		if err != nil {
			log.ERROR.Printf("create heartbeat error: %v", err)
		}
		//TODO heartbeat设置tasknum
		monitor.Broker.SendHeartbeat(heartBeat, monitor.Broker.GetConfig().MonitorServerQueue)
	})
	if err != nil {
		log.ERROR.Printf("add monitor error: %s", err)
		return err
	}
	c.Start()

	errorsChan := make(chan error)
	for {
		select {
		case <-context.Background().Done():
			return nil
		case <-errorsChan:
			return fmt.Errorf("receive heartbeat error %v", errorsChan)
		default:
			heartbeat, err := monitor.Broker.ConsumeHeartbeat(monitor.Broker.GetConfig().MonitorWorkerQueue)
			if err == nil || err.Error() == "redigo: nil returned" {
				if heartbeat != nil {
					_ = append(Machines[heartbeat.MachineType], heartbeat)
					log.INFO.Printf("receive a worker heartbeat %v", heartbeat)
					// TODO 收听到心跳后对worker做全面的分析诊断
				}
				continue
			} else {
				errorsChan <- err
			}
		}
	}
	return nil
}

type serverAlive struct {
	sync.Mutex
	Num int
}
