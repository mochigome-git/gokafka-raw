package realtime

import (
	"context"
	"fmt"
	"gokafka-raw/internal/db"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // adjust for prod
	},
}

func VerifySupabaseToken(tokenString string) (jwt.MapClaims, error) {

	if CachedJWKS == nil {
		return nil, fmt.Errorf("JWKS not loaded")
	}

	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (any, error) {
		kid, ok := token.Header["kid"].(string)
		if !ok {
			return nil, fmt.Errorf("missing kid")
		}

		for _, key := range CachedJWKS.Keys {
			if key.Kid == kid {
				switch token.Header["alg"] {
				case "RS256":
					return key.RSAPublicKey()
				case "ES256":
					return key.ECDSAPublicKey()
				case "HS256":
					return []byte("LEGACY_HS256_SECRET"), nil
				default:
					return nil, fmt.Errorf("unsupported alg: %s", token.Header["alg"])
				}
			}
		}

		return nil, fmt.Errorf("key not found")
	})

	if err != nil || !token.Valid {
		return nil, fmt.Errorf("invalid token: %w", err)
	}

	claims := token.Claims.(jwt.MapClaims)

	// Optional: enforce issuer & audience
	if claims["aud"] != "authenticated" {
		return nil, fmt.Errorf("invalid audience: %v", claims["aud"])
	}

	return claims, nil
}

func ServeWS(hub *Hub, dbMgr *db.DBManager, w http.ResponseWriter, r *http.Request) {
	// test connection usage
	// wscat -c "ws://localhost/ws?token={jwt_token}"
	// {"action":"subscribe","device_ids":["8018052b-45f8-46c3-b2fd-3ac2c15cd6f4"]}

	token := r.URL.Query().Get("token")

	if token == "" {
		authHeader := r.Header.Get("Authorization")
		if authHeader != "" {
			token = strings.TrimPrefix(authHeader, "Bearer ")
		}
	}
	if token == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	claims, err := VerifySupabaseToken(token)
	if err != nil {
		http.Error(w, "Invalid token", http.StatusUnauthorized)
		return
	}

	userID := claimString(claims, "sub")

	tenantID, err := db.SelectTenantIDByUserID(
		context.Background(),
		dbMgr.Pool(),
		userID,
	)
	if err != nil {
		http.Error(w, "Tenant not found", http.StatusUnauthorized)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("Upgrade error:", err)
		return
	}

	client := NewClient(conn, hub, userID, tenantID)

	hub.mu.Lock()
	hub.clientSubs[client] = make(map[SubscriptionKey]bool)
	hub.mu.Unlock()

	defer func() {
		fmt.Println("Client disconnected:", conn.RemoteAddr())
		hub.Unsubscribe(client)
		client.conn.Close()
	}()

	go client.WritePump()
	client.ReadPump()
}

func (h *Hub) IsDeviceAllowed(tenantID, deviceID string) bool {
	// TODO: replace with DB/cache lookup
	return true
}
