package realtime

import (
	"sync"

	"go.uber.org/zap"
)

type SubscriptionKey struct {
	TenantID string
	DeviceID string // keep field name for backward compatibility
}

type Hub struct {
	mu sync.RWMutex

	// room mapping: (tenant+device or tenant+machine) -> clients
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

// Subscribe adds a client to a subscription key
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

// Unsubscribe removes a client from all subscriptions
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

// BroadcastTo sends message to all clients subscribed to either deviceID or machineID
func (h *Hub) BroadcastTo(tenantID, deviceID, machineID string, msg []byte) {
	// broadcast by deviceID
	h.broadcastKey(SubscriptionKey{TenantID: tenantID, DeviceID: deviceID}, msg)
	// broadcast by machineID
	h.broadcastKey(SubscriptionKey{TenantID: tenantID, DeviceID: machineID}, msg)
}

// broadcastKey sends message to clients for a single subscription key
func (h *Hub) broadcastKey(key SubscriptionKey, msg []byte) {
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
			// skip slow client
		}
	}
}

// IsDeviceAllowed checks if client can subscribe to a device or machine
func (h *Hub) IsDeviceAllowed(tenantID, deviceID string) bool {
	// TODO: replace with DB/cache lookup
	return true
}
