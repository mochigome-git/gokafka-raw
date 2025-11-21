package realtime

import (
	"sync"

	"go.uber.org/zap"
)

type SubscriptionKey struct {
	TenantID string
	DeviceID string
}

type Hub struct {
	mu sync.RWMutex

	// room mapping: (tenant+device) -> clients
	rooms map[SubscriptionKey]map[*Client]bool

	// reverse mapping: client -> subscribed keys
	clientSubs map[*Client]map[SubscriptionKey]bool
	logger     *zap.SugaredLogger
}

func NewHub(logger *zap.SugaredLogger) *Hub {
	return &Hub{
		rooms:      make(map[SubscriptionKey]map[*Client]bool),
		clientSubs: make(map[*Client]map[SubscriptionKey]bool),
		logger:     logger,
	}
}

func (h *Hub) Subscribe(key SubscriptionKey, c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.rooms[key] == nil {
		h.rooms[key] = make(map[*Client]bool)
	}
	h.rooms[key][c] = true

	if h.clientSubs[c] == nil {
		h.clientSubs[c] = make(map[SubscriptionKey]bool)
	}
	h.clientSubs[c][key] = true
}

func (h *Hub) Unsubscribe(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	subs := h.clientSubs[c]
	for key := range subs {
		delete(h.rooms[key], c)
		if len(h.rooms[key]) == 0 {
			delete(h.rooms, key)
		}
	}

	delete(h.clientSubs, c)
}

func (h *Hub) BroadcastTo(tenantID, deviceID string, msg []byte) {
	key := SubscriptionKey{TenantID: tenantID, DeviceID: deviceID}

	h.mu.RLock()
	defer h.mu.RUnlock()

	clients := h.rooms[key]
	if clients == nil {
		return
	}

	for client := range clients {
		select {
		case client.send <- msg:
			// delivered
		default:
			// slow client -> skip

		}
	}
}
