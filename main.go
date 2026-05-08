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
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all for prototyping
	},
}

type Client struct {
	conn     *websocket.Conn
	Username string
	LocalIPs []string
	PublicIP string
}

type Message struct {
	Type    string      `json:"type"` // "register", "clients", "call_request", "call_response"
	Payload interface{} `json:"payload"`
}

type RegisterPayload struct {
	Username string   `json:"username"`
	LocalIPs []string `json:"local_ips"`
}

type CallRequestPayload struct {
	TargetUsername string `json:"target_username"`
}

var (
	clients = make(map[*websocket.Conn]*Client)
	mu      sync.Mutex

	relayAddress string
)

func main() {
	port := flag.Int("port", 8080, "Port to run the lobby server on")
	flag.StringVar(&relayAddress, "relay", "ws://localhost:8081/ws", "Address of the relay server to provide out to clients")
	flag.Parse()

	http.HandleFunc("/ws", handleConnections)

	log.Printf("Starting Lobby Server on :%d\n", *port)
	err := http.ListenAndServe(fmt.Sprintf(":%d", *port), nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	defer ws.Close()

	// Capture the public IP of the connecting client
	publicIP, _, _ := net.SplitHostPort(r.RemoteAddr)

	client := &Client{
		conn:     ws,
		PublicIP: publicIP,
	}

	mu.Lock()
	clients[ws] = client
	mu.Unlock()

	defer func() {
		mu.Lock()
		delete(clients, ws)
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
			log.Println("Invalid message format:", err)
			continue
		}

		handleMessage(client, msg)
	}
}

func handleMessage(client *Client, msg Message) {
	switch msg.Type {
	case "register":
		var payload RegisterPayload
		b, _ := json.Marshal(msg.Payload)
		json.Unmarshal(b, &payload)

		client.Username = payload.Username
		client.LocalIPs = payload.LocalIPs

		log.Printf("Client registered: %s (Public IP: %s, Local IPs: %v)", client.Username, client.PublicIP, client.LocalIPs)
		broadcastClients()

	case "call":
		var payload CallRequestPayload
		b, _ := json.Marshal(msg.Payload)
		json.Unmarshal(b, &payload)

		log.Printf("Received call request from %s to %s", client.Username, payload.TargetUsername)

		mu.Lock()
		var targetClient *Client
		for _, c := range clients {
			if c.Username == payload.TargetUsername {
				targetClient = c
				break
			}
		}
		mu.Unlock()

		if targetClient == nil {
			client.conn.WriteJSON(Message{
				Type: "error",
				Payload: map[string]string{
					"message": "User not found",
				},
			})
			return
		}

		routeType := "relay"
		if client.PublicIP == targetClient.PublicIP {
			routeType = "lan"
		}

		sessionID := fmt.Sprintf("%s-%s-%d", client.Username, targetClient.Username, time.Now().UnixNano())

		// Notify the caller
		client.conn.WriteJSON(Message{
			Type: "call_routing",
			Payload: map[string]interface{}{
				"target_username": targetClient.Username,
				"route_type":      routeType,
				"target_ips":      targetClient.LocalIPs,
				"target_public":   targetClient.PublicIP,
				"relay_address":   relayAddress,
				"session_id":      sessionID,
			},
		})

		// Notify the target
		targetClient.conn.WriteJSON(Message{
			Type: "incoming_call",
			Payload: map[string]interface{}{
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
	mu.Lock()
	defer mu.Unlock()

	var activeClients []map[string]interface{}
	for _, c := range clients {
		if c.Username != "" {
			activeClients = append(activeClients, map[string]interface{}{
				"username":  c.Username,
				"public_ip": c.PublicIP,
			})
		}
	}

	msg := Message{
		Type:    "clients",
		Payload: activeClients,
	}

	for ws := range clients {
		err := ws.WriteJSON(msg)
		if err != nil {
			log.Printf("Error sending clients to %s: %v", clients[ws].Username, err)
			ws.Close()
		}
	}
}
