package realtime

import (
	"encoding/json"
	"fmt"
	"gokafka-raw/internal/db"

	"github.com/gorilla/websocket"
)

type Client struct {
	conn     *websocket.Conn
	send     chan []byte
	userID   string
	tenantID string
	hub      *Hub
	DBMgr    *db.DBManager
}

func NewClient(conn *websocket.Conn, hub *Hub, userID, tenantID string) *Client {
	return &Client{
		conn:     conn,
		send:     make(chan []byte, 256),
		hub:      hub,
		userID:   userID,
		tenantID: tenantID,
	}
}

type SubscribeMessage struct {
	Action    string   `json:"action"`
	DeviceIDs []string `json:"device_ids"`
}

func (c *Client) ReadPump() {
	defer func() {
		fmt.Println("ReadPump exiting for user:", c.userID)
		c.hub.Unsubscribe(c)
		c.conn.Close()
	}()

	for {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			c.hub.logger.Warnw("ReadMessage error", "error", err)
			break
		}

		var req SubscribeMessage
		if err := json.Unmarshal(msg, &req); err != nil {
			c.hub.logger.Warnw("JSON unmarshal error", "error", err)
			continue
		}

		if req.Action == "subscribe" {
			for _, deviceID := range req.DeviceIDs {

				// Verify device ownership before subscribing
				if !c.hub.IsDeviceAllowed(c.tenantID, deviceID) {
					fmt.Println("❌ Device not allowed:", "userID", c.userID,
						"deviceID", deviceID)
					continue
				}

				fmt.Println("🟢 Subscribing to device:", "userID", c.userID,
					"deviceID", deviceID)
				c.hub.Subscribe(SubscriptionKey{
					TenantID: c.tenantID,
					DeviceID: deviceID,
				}, c)
			}
		} else {
			c.hub.logger.Errorw("Unknown action:", req.Action)
		}
	}
}

func (c *Client) WritePump() {
	fmt.Println("🟢 WritePump started for user:", c.userID)
	for msg := range c.send {
		if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			c.hub.logger.Errorw("WriteMessage error:", err)
			break
		}
	}
	c.hub.logger.Info("WritePump exiting")
}
