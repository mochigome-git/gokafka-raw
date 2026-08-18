package service

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"gokafka-raw/internal/db"
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

// commitUpdate carries a newly-committable offset to the single
// committer goroutine.
type commitUpdate struct {
	topic     string
	partition int
	offset    int64
}

func (s *KafkaService) processWorker() {
	atomic.AddInt32(&s.activeProcessWorkers, 1)
	defer atomic.AddInt32(&s.activeProcessWorkers, -1)

	for job := range s.processCh {
		s.pending.Add(1)
		s.handleMessage(job)

		// Per-message goroutine only WAITS and forwards the safe offset
		// to the single committer — no broker round-trip here anymore
		// (CHANGED: previously each of these called CommitMessages
		// directly, meaning commit-RPC rate == message rate and a data
		// race on s.Reader).
		go func(m kafka.Message, wg *sync.WaitGroup) {
			defer s.pending.Done()
			wg.Wait()
			if safe := s.offsets.markDone(m.Partition, m.Offset); safe >= 0 {
				select {
				case s.commitCh <- commitUpdate{topic: m.Topic, partition: m.Partition, offset: safe}:
				default:
					// commitCh full: fine — a later, higher safe offset
					// will supersede this one. Committing is monotonic;
					// skipping an intermediate value loses nothing.
				}
			}
		}(job.Msg, job.Wg)
	}
}

// committer is the ONLY goroutine that talks to reader.CommitMessages.
// It debounces: keeps the highest safe offset per partition and flushes
// at most once per interval. Uses context.Background() with a timeout so
// shutdown cancellation can't strand the final commits (CHANGED — the old
// per-message commits used the consumer ctx, which cancels on shutdown
// and caused redelivery of the whole last batch on every restart).
func (s *KafkaService) committer(reader *kafka.Reader) {
	const flushEvery = 1 * time.Second
	latest := make(map[int]commitUpdate) // partition → highest safe

	ticker := time.NewTicker(flushEvery)
	defer ticker.Stop()

	flush := func() {
		if len(latest) == 0 {
			return
		}
		msgs := make([]kafka.Message, 0, len(latest))
		for _, u := range latest {
			msgs = append(msgs, kafka.Message{Topic: u.topic, Partition: u.partition, Offset: u.offset})
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := reader.CommitMessages(ctx, msgs...); err != nil {
			s.Logger.Errorw("failed to commit offsets", "error", err, "batch", len(msgs))
			// keep `latest` — retry same offsets on next tick
		} else {
			latest = make(map[int]commitUpdate)
		}
		cancel()
	}

	for {
		select {
		case u, ok := <-s.commitCh:
			if !ok {
				flush() // final flush on shutdown
				return
			}
			if cur, exists := latest[u.partition]; !exists || u.offset > cur.offset {
				latest[u.partition] = u
			}
		case <-ticker.C:
			flush()
		}
	}
}

// StartDepthLogger periodically logs channel depths so backpressure
// stalls (DB down → channels full → consume loop blocked) are visible
// instead of looking like "consumer stopped, no errors" (CHANGED — new).
func (s *KafkaService) StartDepthLogger(ctx context.Context, every time.Duration) {
	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.Logger.Infow("pipeline depths",
					"processCh", len(s.processCh),
					"telemetryCh", len(s.telemetryCh),
					"realtimeCh", len(s.realtimeCh),
					"eventCh", len(s.eventCh),
					"jobCh", len(s.jobCh),
					"commitCh", len(s.commitCh),
					"processWorkers", atomic.LoadInt32(&s.activeProcessWorkers),
					"insertWorkers", atomic.LoadInt32(&s.activeInsertWorkers),
				)
			}
		}
	}()
}

// insertWorker takes its own per-type semaphore.
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
		s.Logger.Errorw("failed to parse wrapper message", "error", err,
			"partition", job.Msg.Partition, "offset", job.Msg.Offset)
		s.deadLetter(job.Msg, "parse", "", nil, nil, err)
		return
	}

	var msg model.TelemetryMessage
	if err := jsonFast.Unmarshal([]byte(wrapper.Payload), &msg); err != nil {
		s.Logger.Errorw("failed to parse telemetry payload", "error", err,
			"partition", job.Msg.Partition, "offset", job.Msg.Offset)
		s.deadLetter(job.Msg, "parse", msg.TenantID, msg.DeviceID, nil, err)
		return
	}

	s.queueInserts(msg, job.Msg, job.Ctx, job.Stats, job.Wg)
}

// deadLetter preserves a permanently-failed message in ingest.dead_letters
// (CHANGED — new). Uses context.Background(): the DLQ write must survive
// consumer-context cancellation, since shutdown is exactly when in-flight
// failures are most likely.
func (s *KafkaService) deadLetter(m kafka.Message, insertType string,
	tenantID string, deviceID *string, jobRef *string, cause error) {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = db.InsertDeadLetter(ctx, s.DBMgr.Pool(), db.DeadLetter{
		Topic:       m.Topic,
		Partition:   m.Partition,
		KafkaOffset: m.Offset,
		TenantID:    tenantID,
		DeviceID:    deviceID,
		JobRef:      jobRef,
		InsertType:  insertType,
		RawPayload:  m.Value,
		LastError:   cause.Error(),
	}, s.Logger)
	// InsertDeadLetter already logs CRITICAL with the full payload if
	// even the DLQ write fails — nothing more to do here.
}

func isHeartbeat(msg model.TelemetryMessage) bool {
	if len(msg.Status) == 0 {
		return false
	}
	var st struct {
		Kind *string `json:"kind"`
	}
	if err := jsonFast.Unmarshal(msg.Status, &st); err != nil {
		return false
	}
	return st.Kind != nil && *st.Kind == "heartbeat"
}
