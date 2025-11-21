package service

import (
	"gokafka-raw/internal/model"
	"sync/atomic"

	jsoniter "github.com/json-iterator/go"
)

func resolveEntityID(msg model.TelemetryMessage) string {
	if msg.DeviceID != nil && *msg.DeviceID != "" {
		return *msg.DeviceID
	}
	if msg.MachineID != nil && *msg.MachineID != "" {
		return *msg.MachineID
	}
	return ""
}

// Worker loops
func (s *KafkaService) processWorker() {
	atomic.AddInt32(&s.activeProcessWorkers, 1)
	defer atomic.AddInt32(&s.activeProcessWorkers, -1)

	for job := range s.processCh {
		s.handleMessage(job)
	}
}

// insertWorker with atomic active count + semaphore
func (s *KafkaService) insertWorker(ch chan func()) {
	atomic.AddInt32(&s.activeInsertWorkers, 1)
	defer atomic.AddInt32(&s.activeInsertWorkers, -1)

	for job := range ch {
		s.insertSem <- struct{}{} // acquire semaphore
		job()
		<-s.insertSem // release
	}
}

// Handle message & queue inserts
var (
	jsonFast = jsoniter.ConfigFastest
)

func (s *KafkaService) handleMessage(job ProcessJob) {
	var wrapper model.KafkaWrapper
	if err := jsonFast.Unmarshal(job.Msg.Value, &wrapper); err != nil {
		s.Logger.Errorw("failed to parse wrapper message", "error", err)
		return
	}

	var msg model.TelemetryMessage
	if err := jsonFast.Unmarshal([]byte(wrapper.Payload), &msg); err != nil {
		s.Logger.Errorw("failed to parse telemetry payload", "error", err)
		return
	}

	s.queueInserts(msg, job.Msg, job.Ctx, job.Stats)
}
