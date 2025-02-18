package redis

import (
	"bytes"
	"encoding/json"
	"github.com/go-redis/redis"
	"sync"
	"time"

	"github.com/piaobeizu/machinery/v1/backends/iface"
	"github.com/piaobeizu/machinery/v1/common"
	"github.com/piaobeizu/machinery/v1/config"
	"github.com/piaobeizu/machinery/v1/log"
	"github.com/piaobeizu/machinery/v1/tasks"
	"github.com/RichardKnop/redsync"
)

// Backend represents a Redis result backend
type BackendGR struct {
	common.Backend
	rclient  redis.UniversalClient
	host     string
	password string
	db       int
	// If set, path to a socket file overrides hostname
	socketPath string
	redsync    *redsync.Redsync
	redisOnce  sync.Once
}

// New creates Backend instance
func NewGR(cnf *config.Config, addrs []string, db int) iface.Backend {
	b := &BackendGR{
		Backend: common.NewBackend(cnf),
	}
	ropt := &redis.UniversalOptions{
		Addrs: addrs,
		DB:    db,
	}
	b.rclient = redis.NewUniversalClient(ropt)
	return b
}

// InitGroup creates and saves a group meta data object
func (b *BackendGR) InitGroup(groupUUID string, taskUUIDs []string) error {
	groupMeta := &tasks.GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
		CreatedAt: time.Now().UTC(),
	}

	encoded, err := json.Marshal(groupMeta)
	if err != nil {
		return err
	}

	err = b.rclient.Set(groupUUID, encoded, 0).Err()
	if err != nil {
		return err
	}

	return b.setExpirationTime(groupUUID)
}

// GroupCompleted returns true if all tasks in a group finished
func (b *BackendGR) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	taskStates, err := b.getStates(groupMeta.TaskUUIDs...)
	if err != nil {
		return false, err
	}

	var countSuccessTasks = 0
	for _, taskState := range taskStates {
		if taskState.IsCompleted() {
			countSuccessTasks++
		}
	}

	return countSuccessTasks == groupTaskCount, nil
}

// GroupTaskStates returns states of all tasks in the group
func (b *BackendGR) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return []*tasks.TaskState{}, err
	}

	return b.getStates(groupMeta.TaskUUIDs...)
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *BackendGR) TriggerChord(groupUUID string) (bool, error) {
	m := b.redsync.NewMutex("TriggerChordMutex")
	if err := m.Lock(); err != nil {
		return false, err
	}
	defer m.Unlock()

	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	// Chord has already been triggered, return false (should not trigger again)
	if groupMeta.ChordTriggered {
		return false, nil
	}

	// Set flag to true
	groupMeta.ChordTriggered = true

	// Update the group meta
	encoded, err := json.Marshal(&groupMeta)
	if err != nil {
		return false, err
	}

	err = b.rclient.Set(groupUUID, encoded, 0).Err()
	if err != nil {
		return false, err
	}

	return true, b.setExpirationTime(groupUUID)
}

func (b *BackendGR) mergeNewTaskState(newState *tasks.TaskState) {
	state, err := b.GetState(newState.TaskUUID)
	if err == nil {
		newState.CreatedAt = state.CreatedAt
		newState.TaskName = state.TaskName
	}
}

// SetStatePending updates task state to PENDING
func (b *BackendGR) SetStatePending(signature *tasks.Signature) error {
	taskState := tasks.NewPendingTaskState(signature)
	return b.updateState(taskState)
}

// SetStateReceived updates task state to RECEIVED
func (b *BackendGR) SetStateReceived(signature *tasks.Signature) error {
	taskState := tasks.NewReceivedTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateStarted updates task state to STARTED
func (b *BackendGR) SetStateStarted(signature *tasks.Signature) error {
	taskState := tasks.NewStartedTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateRetry updates task state to RETRY
func (b *BackendGR) SetStateRetry(signature *tasks.Signature) error {
	taskState := tasks.NewRetryTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateSuccess updates task state to SUCCESS
func (b *BackendGR) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	taskState := tasks.NewSuccessTaskState(signature, results)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateFailure updates task state to FAILURE
func (b *BackendGR) SetStateFailure(signature *tasks.Signature, err string) error {
	taskState := tasks.NewFailureTaskState(signature, err)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// GetState returns the latest task state
func (b *BackendGR) GetState(taskUUID string) (*tasks.TaskState, error) {

	item, err := b.rclient.Get(taskUUID).Bytes()
	if err != nil {
		return nil, err
	}
	state := new(tasks.TaskState)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(state); err != nil {
		return nil, err
	}

	return state, nil
}

// PurgeState deletes stored task state
func (b *BackendGR) PurgeState(taskUUID string) error {
	err := b.rclient.Del(taskUUID).Err()
	if err != nil {
		return err
	}

	return nil
}

// PurgeGroupMeta deletes stored group meta data
func (b *BackendGR) PurgeGroupMeta(groupUUID string) error {
	err := b.rclient.Del(groupUUID).Err()
	if err != nil {
		return err
	}

	return nil
}

// getGroupMeta retrieves group meta data, convenience function to avoid repetition
func (b *BackendGR) getGroupMeta(groupUUID string) (*tasks.GroupMeta, error) {
	item, err := b.rclient.Get(groupUUID).Bytes()
	if err != nil {
		return nil, err
	}

	groupMeta := new(tasks.GroupMeta)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(groupMeta); err != nil {
		return nil, err
	}

	return groupMeta, nil
}

// getStates returns multiple task states
func (b *BackendGR) getStates(taskUUIDs ...string) ([]*tasks.TaskState, error) {
	taskStates := make([]*tasks.TaskState, len(taskUUIDs))
	// to avoid CROSSSLOT error, use pipeline
	cmders, err := b.rclient.Pipelined(func(pipeliner redis.Pipeliner) error {
		for _, uuid := range taskUUIDs {
			pipeliner.Get(uuid)
		}
		return nil
	})
	if err != nil {
		return taskStates, err
	}
	for i, cmder := range cmders {
		stateBytes, err1 := cmder.(*redis.StringCmd).Bytes()
		if err1 != nil {
			return taskStates, err1
		}
		taskState := new(tasks.TaskState)
		decoder := json.NewDecoder(bytes.NewReader(stateBytes))
		decoder.UseNumber()
		if err1 = decoder.Decode(taskState); err1 != nil {
			log.ERROR.Print(err1)
			return taskStates, err1
		}
		taskStates[i] = taskState
	}

	return taskStates, nil
}

// updateState saves current task state
func (b *BackendGR) updateState(taskState *tasks.TaskState) error {
	encoded, err := json.Marshal(taskState)
	if err != nil {
		return err
	}

	_, err = b.rclient.Set(taskState.TaskUUID, encoded, 0).Result()
	if err != nil {
		return err
	}

	return b.setExpirationTime(taskState.TaskUUID)
}

// setExpirationTime sets expiration timestamp on a stored task state
func (b *BackendGR) setExpirationTime(key string) error {
	expiresIn := b.GetConfig().ResultsExpireIn
	if expiresIn == 0 {
		// // expire results after 1 hour by default
		expiresIn = config.DefaultResultsExpireIn
	}
	expirationTimestamp := time.Now().Add(time.Duration(expiresIn) * time.Second)

	_, err := b.rclient.ExpireAt(key, expirationTimestamp).Result()
	if err != nil {
		return err
	}

	return nil
}
