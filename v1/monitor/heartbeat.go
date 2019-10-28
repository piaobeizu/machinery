package monitor

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
	"math"
	"net"
	"strings"
	"time"
)

type MACHINE_TYPE string

//TODO 丰富machine type
const (
	MACHINERY_SERVER MACHINE_TYPE = "machinery-server" //server节点
	SPARK_CLUSTER    MACHINE_TYPE = "spark-cluster"    //spark集群
	FLINK_CLUSTER    MACHINE_TYPE = "flink-cluster"    //flink集群
	HADOOP_CLUSTER   MACHINE_TYPE = "hadoop-cluster"   //flink集群
	K8S_CLUSTER      MACHINE_TYPE = "k8s-cluster"      //k8s集群
	OTHER_CLUSTER    MACHINE_TYPE = "other-cluster"    //其他
)

// TODO 丰富监听心跳的信息
type Heartbeat struct {
	UUID        string
	WorkerName  string
	MachineType MACHINE_TYPE
	Ip          string
	CpuNum      int
	CpuUsage    float64
	MemNum      string
	MemUsage    float64
	TaskNum     int
	Timestamp   int64
}

// NewSignature creates a new heartbeat signature
func NewHeartBeat(workerName string, machineType MACHINE_TYPE) (*Heartbeat, error) {
	m, _ := mem.VirtualMemory()
	cores, _ := cpu.Counts(false)
	cpuUsage, err := cpu.Percent(10*time.Millisecond, false)
	if err != nil {
		return nil, err
	}
	ips := getIPs()
	macAddrs := getMacAddrs()
	return &Heartbeat{
		UUID:        get16MD5Encode(strings.Join(macAddrs, "") + strings.Join(ips, "")),
		Ip:          strings.Join(ips, ","),
		MachineType: machineType,
		WorkerName:  workerName,
		CpuNum:      cores,
		CpuUsage:    math.Trunc(cpuUsage[0]*1e2) * 1e-2,
		MemNum:      fmt.Sprintf("%v", m.Total),
		MemUsage:    math.Trunc(m.UsedPercent*1e2) * 1e-2,
		Timestamp:   time.Now().In(time.Local).Unix(),
		TaskNum:     0,
	}, nil
}

// NewSignature creates a new heartbeat signature
func NewDefaultHeartBeat(machineType MACHINE_TYPE) (*Heartbeat, error) {
	ips := getIPs()
	macAddrs := getMacAddrs()
	return &Heartbeat{
		UUID:        get16MD5Encode(strings.Join(macAddrs, "") + strings.Join(ips, "")),
		Ip:          strings.Join(ips, ","),
		MachineType: machineType,
		CpuNum:      1,
		CpuUsage:    0.5,
		MemNum:      fmt.Sprintf("%v", "16G"),
		MemUsage:    0.5,
		Timestamp:   time.Now().In(time.Local).Unix(),
		TaskNum:     0,
	}, nil
}

func getMacAddrs() (macAddrs []string) {
	netInterfaces, err := net.Interfaces()
	if err != nil {
		fmt.Printf("fail to get net interfaces: %v", err)
		return macAddrs
	}

	for _, netInterface := range netInterfaces {
		macAddr := netInterface.HardwareAddr.String()
		if len(macAddr) == 0 {
			continue
		}

		macAddrs = append(macAddrs, macAddr)
	}
	return macAddrs
}

func getIPs() (ips []string) {
	interfaceAddr, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Printf("fail to get net interface addrs: %v", err)
		return ips
	}

	for _, address := range interfaceAddr {
		ipNet, isValidIpNet := address.(*net.IPNet)
		if isValidIpNet && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				ips = append(ips, ipNet.IP.String())
			}
		}
	}
	return ips
}

func get16MD5Encode(data string) string {
	h := md5.New()
	h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))[8:24]
}
