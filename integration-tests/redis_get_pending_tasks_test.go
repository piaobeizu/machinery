package integration_test

import (
	"fmt"
	"testing"

	"github.com/piaobeizu/machinery/v1/config"
)

func TestRedisGetPendingTasks(t *testing.T) {
	//redisURL := os.Getenv("REDIS_URL")
	redisURL := "123456@10.70.19.170:6379/10"
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	// Redis broker, Redis result backend
	server := testSetup(&config.Config{
		Broker:        fmt.Sprintf("redis://%v", redisURL),
		DefaultQueue:  "default_queue",
		ResultBackend: fmt.Sprintf("redis://%v", redisURL),
	})
	pendingMessages, err := server.GetBroker().GetPendingTasks(server.GetConfig().DefaultQueue)
	for i := 0; i < len(pendingMessages); i++ {
		fmt.Printf("%+.v\n", pendingMessages[i])
	}

	if err != nil {
		t.Error(err)
	}
	if len(pendingMessages) != 0 {
		t.Errorf(
			"%d pending messages, should be %d",
			len(pendingMessages),
			0,
		)
	}
}
