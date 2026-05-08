package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Client now owns a send channel — one writer goroutine per connection.
type Client struct {
	conn     *websocket.Conn
	send     chan []byte // buffered; writer goroutine drains this
	Username string
	LocalIPs []string
	PublicIP string
}

// Use RawMessage so we never double-encode/decode the payload.
type Message struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type RegisterPayload struct {
	Username string   `json:"username"`
	LocalIPs []string `json:"local_ips"`
}

type CallRequestPayload struct {
	TargetUsername string `json:"target_username"`
}

var (
	// RWMutex: reads (lookup, broadcast snapshot) no longer block each other.
	clients      = make(map[*websocket.Conn]*Client)
	mu           sync.RWMutex
	relayAddress string
)

func main() {
	port := flag.Int("port", 8080, "Port to run the lobby server on")
	flag.StringVar(&relayAddress, "relay", "ws://localhost:8081/ws", "Relay server address")
	flag.Parse()

	http.HandleFunc("/ws", handleConnections)
	log.Printf("Lobby server listening on :%d", *port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", *port), nil); err != nil {
		log.Fatal(err)
	}
}

// writePump serializes all writes for one connection.
// Gorilla websocket requires that only one goroutine writes at a time.
func (c *Client) writePump() {
	defer c.conn.Close()
	for data := range c.send {
		if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
			return
		}
	}
}

// sendJSON encodes v and queues it for the client's writer goroutine.
// Non-blocking: drops the message if the buffer is full (slow client).
func (c *Client) sendJSON(v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	select {
	case c.send <- data:
	default:
		log.Printf("send buffer full for %s, dropping message", c.Username)
	}
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}

	publicIP, _, _ := net.SplitHostPort(r.RemoteAddr)
	client := &Client{
		conn:     ws,
		send:     make(chan []byte, 64), // buffer up to 64 outbound messages
		PublicIP: publicIP,
	}

	mu.Lock()
	clients[ws] = client
	mu.Unlock()

	// One goroutine owns all writes for this connection.
	go client.writePump()

	defer func() {
		mu.Lock()
		delete(clients, ws)
		close(client.send) // signals writePump to exit
		mu.Unlock()
		broadcastClients()
		log.Printf("Client disconnected: %s", client.Username)
	}()

	for {
		_, msgBytes, err := ws.ReadMessage()
		if err != nil {
			break
		}
		var msg Message
		if err := json.Unmarshal(msgBytes, &msg); err != nil {
			log.Println("Invalid message:", err)
			continue
		}
		handleMessage(client, msg)
	}
}

func handleMessage(client *Client, msg Message) {
	switch msg.Type {
	case "register":
		var payload RegisterPayload
		if err := json.Unmarshal(msg.Payload, &payload); err != nil {
			return
		}
		client.Username = payload.Username
		client.LocalIPs = payload.LocalIPs
		log.Printf("Registered: %s (public: %s, local: %v)", client.Username, client.PublicIP, client.LocalIPs)
		broadcastClients()

	case "call":
		var payload CallRequestPayload
		if err := json.Unmarshal(msg.Payload, &payload); err != nil {
			return
		}
		log.Printf("Call request: %s → %s", client.Username, payload.TargetUsername)

		// Read lock is enough — we're only looking up.
		mu.RLock()
		var target *Client
		for _, c := range clients {
			if c.Username == payload.TargetUsername {
				target = c
				break
			}
		}
		mu.RUnlock()

		if target == nil {
			client.sendJSON(map[string]interface{}{
				"type":    "error",
				"payload": map[string]string{"message": "User not found"},
			})
			return
		}

		routeType := "relay"
		if client.PublicIP == target.PublicIP {
			routeType = "lan"
		}
		sessionID := fmt.Sprintf("%s-%s-%d", client.Username, target.Username, time.Now().UnixNano())

		client.sendJSON(map[string]interface{}{
			"type": "call_routing",
			"payload": map[string]interface{}{
				"target_username": target.Username,
				"route_type":      routeType,
				"target_ips":      target.LocalIPs,
				"target_public":   target.PublicIP,
				"relay_address":   relayAddress,
				"session_id":      sessionID,
			},
		})
		target.sendJSON(map[string]interface{}{
			"type": "incoming_call",
			"payload": map[string]interface{}{
				"caller_username": client.Username,
				"route_type":      routeType,
				"caller_ips":      client.LocalIPs,
				"caller_public":   client.PublicIP,
				"relay_address":   relayAddress,
				"session_id":      sessionID,
			},
		})
	}
}

func broadcastClients() {
	// 1. Build the payload under a read lock.
	mu.RLock()
	active := make([]map[string]interface{}, 0, len(clients))
	snapshot := make([]*Client, 0, len(clients))
	for _, c := range clients {
		if c.Username != "" {
			active = append(active, map[string]interface{}{
				"username":  c.Username,
				"public_ip": c.PublicIP,
			})
			snapshot = append(snapshot, c)
		}
	}
	mu.RUnlock()

	// 2. Encode once — reuse the same bytes for every client.
	data, err := json.Marshal(map[string]interface{}{
		"type":    "clients",
		"payload": active,
	})
	if err != nil {
		return
	}

	// 3. Queue to each client's channel — no lock held, no I/O blocking.
	for _, c := range snapshot {
		select {
		case c.send <- data:
		default:
			log.Printf("broadcast: send buffer full for %s", c.Username)
		}
	}
}
