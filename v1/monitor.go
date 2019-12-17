package machinery

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gomodule/redigo/redis"
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
	serverAlive := new(serverAlive)
	serverAlive.EnNow(time.Now().Local().Unix())
	// Log some useful information about worker configuration
	log.INFO.Printf("Launching a worker with the following settings:")
	log.INFO.Printf("- Broker: %s", monitor.Broker.GetConfig())
	c := cron.New(cron.WithSeconds())
	_, err := c.AddFunc("*/5 * * * * ?", func() {
		if time.Now().Local().Unix()-serverAlive.DeNow() <=11 {
			log.INFO.Printf("worker [%s] start publish a heartbeat to server", monitor.WorkerName)
			heartBeat, err := m.NewHeartBeat(monitor.WorkerName, monitor.MType)
			if err != nil {
				log.ERROR.Printf("create heartbeat error: %v", err)
			}
			//TODO heartbeat设置tasknum
			monitor.Broker.SendHeartbeat(heartBeat, monitor.Broker.GetConfig().MonitorWorkerQueue)
		} else {
			log.WARNING.Printf("there is no online server, please start it!!!")
		}
	})
	if err != nil {
		log.ERROR.Printf("add worker monitor error: %s", err)
		return err
	}
	c.Start()

	// 监控server心跳
	ServerHeartbeatConsume := func(msg redis.Message) error {
		heartBeat := m.Heartbeat{}
		json.Unmarshal(msg.Data, &heartBeat)
		serverAlive.EnNow(heartBeat.Timestamp)
		return nil
	}
	go func() {
		for {
			ctx := context.Background()
			if _, err := monitor.Broker.ConsumeHeartbeat(ctx, monitor.Broker.GetConfig().MonitorServerQueue, ServerHeartbeatConsume); err != nil {
				log.WARNING.Printf("subscribe err: %v", err)
			}
			time.Sleep(1 * time.Second)
		}
	}()
	return nil
}

// CustomQueue returns Custom Queue of the running worker process
func (monitor *Monitor) ServerMonitor() error {
	c := cron.New(cron.WithSeconds())
	_, err := c.AddFunc("*/10 * * * * ?", func() {
		log.INFO.Printf("server start publish a heartbeat to workers")
		heartBeat, err := m.NewHeartBeat("server", m.MACHINERY_SERVER)
		if err != nil {
			log.ERROR.Printf("create heartbeat error: %v", err)
		}
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
			heartbeat, err := monitor.Broker.ConsumeHeartbeat(context.Background(), monitor.Broker.GetConfig().MonitorWorkerQueue, nil)
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
	Now int64
}

func (s *serverAlive) EnNow(now int64) {
	s.Lock()
	s.Now = now
	s.Unlock()
}
func (s *serverAlive) DeNow() int64 {
	return s.Now
}
