package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/websocket"

	_ "github.com/joho/godotenv/autoload"

	"backend/internal/database"
)

type Server struct {
	port int
	hubs []Hub
	db   database.Service
}

type Message struct {
	Name    string    `json:"name"`
	Message string    `json:"message"`
	Time    time.Time `json:"time"`
	HubName string    `json:"hubname"`
}

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 512

	//Maximum time a hub can persists without any memebers except DEFAULT hub

)

type Hub struct {
	clients      map[*Client]bool
	brodcast     chan []byte
	register     chan *Client
	unregister   chan *Client
	identifier   int64
	name         string
	isInvincible bool
}

func defaultHub() *Hub {
	return &Hub{
		clients:      make(map[*Client]bool),
		brodcast:     make(chan []byte),
		register:     make(chan *Client),
		unregister:   make(chan *Client),
		identifier:   int64(0),
		name:         "Default",
		isInvincible: true,
	}
}
func NewHub(name string, identifier int64) *Hub {
	return &Hub{
		clients:      make(map[*Client]bool),
		brodcast:     make(chan []byte),
		register:     make(chan *Client),
		unregister:   make(chan *Client),
		isInvincible: false,
		identifier:   identifier, //changes
		name:         name,
	}
}
func (h *Hub) run() {
	log.Println("hub running starting of name : ", h.name)
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true

		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			if len(h.clients) == 0 && !h.isInvincible {
				log.Println("Hub is empty closing the hub : ", h.name)
				return
			}

		case message := <-h.brodcast:
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}

		}
	}
}

type Client struct {
	hub  *Hub
	conn *websocket.Conn
	name string
	send chan []byte
}

//Readpump pumps messages from the websocket connection to the hub

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}
		message = bytes.TrimSpace(bytes.Replace(message, []byte{'\n'}, []byte{' '}, -1))
		jsonpacket := &Message{
			Name:    c.name,
			Message: string(message),
			Time:    time.Now(),
			HubName: c.hub.name,
		}
		jsonbytes, err := json.Marshal(jsonpacket)
		if err != nil {
			log.Println(err)
			return
		}

		c.hub.brodcast <- jsonbytes
	}
}

//Write pump pumps messages from the hub to the websocket connection

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The hub closed the channel.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}

			w.Write(message)

			// Add queued chat messages to the current websocket message.
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write([]byte{'\n'})
				w.Write(<-c.send)
			}
			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}

}

var allowedOrigins = map[string]bool{
	"http://localhost:5173":         true,
	"https://dashboard.example.com": true,
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		// Check if the request origin exists in your trusted map
		return allowedOrigins[origin]
	},
}

func NewServer() *http.Server {
	port, _ := strconv.Atoi(os.Getenv("PORT"))
	hub := defaultHub()
	go hub.run()
	NewServer := &Server{
		port: port,
		hubs: []Hub{*hub},
		db:   database.New(),
	}

	// Declare Server config
	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", NewServer.port),
		Handler:      NewServer.RegisterRoutes(),
		IdleTimeout:  time.Minute,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	return server
}
