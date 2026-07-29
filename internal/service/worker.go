package service

import (
	"context"
	"sync"
	"sync/atomic"

	"gokafka-raw/internal/model"

	jsoniter "github.com/json-iterator/go"
	"github.com/segmentio/kafka-go"
)

func resolveEntityID(msg model.TelemetryMessage) string {
	if msg.DeviceID != nil && *msg.DeviceID != "" {
		return *msg.DeviceID
	}
	return ""
}

func (s *KafkaService) processWorker() {
	atomic.AddInt32(&s.activeProcessWorkers, 1)
	defer atomic.AddInt32(&s.activeProcessWorkers, -1)

	for job := range s.processCh {
		s.pending.Add(1)
		s.handleMessage(job)

		go func(m kafka.Message, wg *sync.WaitGroup, ctx context.Context) {
			defer s.pending.Done()
			wg.Wait()
			if safe := s.offsets.markDone(m.Partition, m.Offset); safe >= 0 {
				commitMsg := kafka.Message{Topic: m.Topic, Partition: m.Partition, Offset: safe}
				if err := s.Reader.CommitMessages(ctx, commitMsg); err != nil {
					s.Logger.Errorw("failed to commit offset", "error", err, "partition", m.Partition, "offset", safe)
				}
			}
		}(job.Msg, job.Wg, job.Ctx)
	}
}

// insertWorker now takes its own semaphore (CHANGED — was one shared
// s.insertSem across all 4 channels, which capped total concurrency at
// 10 regardless of channel count).
func (s *KafkaService) insertWorker(ch chan func(), sem chan struct{}) {
	atomic.AddInt32(&s.activeInsertWorkers, 1)
	defer atomic.AddInt32(&s.activeInsertWorkers, -1)

	for job := range ch {
		sem <- struct{}{}
		job()
		<-sem
	}
}

var jsonFast = jsoniter.ConfigFastest

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

	s.queueInserts(msg, job.Msg, job.Ctx, job.Stats, job.Wg)
}

func isHeartbeat(msg model.TelemetryMessage) bool {
	if len(msg.Status) == 0 {
		return false
	}
	var s struct {
		Kind *string `json:"kind"`
	}
	if err := jsonFast.Unmarshal(msg.Status, &s); err != nil {
		return false
	}
	return s.Kind != nil && *s.Kind == "heartbeat"
}
