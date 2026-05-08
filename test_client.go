//go:build ignore

package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

type TestClientMessage struct {
	Type    string      `json:"type"`
	Payload interface{} `json:"payload"`
}

type RoutingPayload struct {
	RouteType    string `json:"route_type"`
	RelayAddress string `json:"relay_address"`
	SessionID    string `json:"session_id"`
}

func main() {
	time.Sleep(1 * time.Second) // Give the server a moment to start if run simultaneously

	// Connect Client Alice
	connA, _, err := websocket.DefaultDialer.Dial("ws://localhost:8080/ws", nil)
	if err != nil {
		log.Fatal("Alice dial error:", err)
	}
	defer connA.Close()

	// Connect Client Bob
	connB, _, err := websocket.DefaultDialer.Dial("ws://localhost:8080/ws", nil)
	if err != nil {
		log.Fatal("Bob dial error:", err)
	}
	defer connB.Close()

	// Start readers that also handle answering the call via relay
	go readMessages("Alice", connA)
	go readMessages("Bob", connB)

	// Register Alice
	log.Println("Registering Alice...")
	connA.WriteJSON(TestClientMessage{
		Type: "register",
		Payload: map[string]interface{}{
			"username":  "Alice",
			"local_ips": []string{"192.168.1.10"},
		},
	})
	time.Sleep(1 * time.Second)

	// Register Bob
	log.Println("Registering Bob...")
	connB.WriteJSON(TestClientMessage{
		Type: "register",
		Payload: map[string]interface{}{
			"username":  "Bob",
			"local_ips": []string{"192.168.1.20"},
		},
	})
	time.Sleep(1 * time.Second)

	// Alice calls Bob
	log.Println("Alice is calling Bob...")
	connA.WriteJSON(TestClientMessage{
		Type: "call",
		Payload: map[string]interface{}{
			"target_username": "Bob",
		},
	})

	time.Sleep(5 * time.Second)
	log.Println("Test finished.")
}

func readMessages(name string, conn *websocket.Conn) {
	for {
		_, msgBytes, err := conn.ReadMessage()
		if err != nil {
			log.Printf("[%s] Read error: %v", name, err)
			return
		}

		var msg TestClientMessage
		json.Unmarshal(msgBytes, &msg)

		log.Printf("[%s] Received from Lobby: type=%s, payload=%v", name, msg.Type, string(msgBytes))

		if msg.Type == "call_routing" || msg.Type == "incoming_call" {
			var routing RoutingPayload
			b, _ := json.Marshal(msg.Payload)
			json.Unmarshal(b, &routing)
			
			// For testing purpose, we connect to relay to test the bridge!
			log.Printf("[%s] Connecting to relay at %s?id=%s", name, routing.RelayAddress, routing.SessionID)
			relayConn, _, err := websocket.DefaultDialer.Dial(routing.RelayAddress+"?id="+routing.SessionID, nil)
			if err != nil {
				log.Printf("[%s] Failed to connect to relay: %v", name, err)
				continue
			}
			
			go handleRelayMessages(name, relayConn)
			
			// Send a test message through the relay
			testStr := "Hello across the relay from " + name
			relayConn.WriteMessage(websocket.TextMessage, []byte(testStr))
			log.Printf("[%s] Sent message to relay: %s", name, testStr)
		}
	}
}

func handleRelayMessages(name string, relayConn *websocket.Conn) {
	for {
		_, msgBytes, err := relayConn.ReadMessage()
		if err != nil {
			log.Printf("[%s] Relay read error: %v", name, err)
			return
		}
		log.Printf("[%s] Received FROM RELAY: %s", name, string(msgBytes))
	}
}
