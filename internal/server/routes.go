package server

import (
	"log"
	"math/big"
	"net/http"
	"strconv"

	"crypto/rand"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func (s *Server) RegisterRoutes() http.Handler {
	r := gin.Default()

	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"http://localhost:5173"}, // Add your frontend URL
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"},
		AllowHeaders:     []string{"Accept", "Authorization", "Content-Type"},
		AllowCredentials: true, // Enable cookies/auth
	}))

	r.GET("/", s.HelloWorldHandler)

	r.GET("/health", s.healthHandler)

	r.GET("/ws", func(ctx *gin.Context) {
		defserveWs(&s.hubs[0], ctx.Writer, ctx.Request, ctx.Query("username"))
	})
	r.GET("/hub/new/:hubname", s.Startnew)

	r.GET("/ws/:hubidentifier", s.extserveWs)

	return r
}

func (s *Server) HelloWorldHandler(c *gin.Context) {
	resp := make(map[string]string)
	resp["message"] = "Hello World"

	c.JSON(http.StatusOK, resp)
}

func (s *Server) healthHandler(c *gin.Context) {
	c.JSON(http.StatusOK, s.db.Health())
}

func defserveWs(hub *Hub, w http.ResponseWriter, r *http.Request, name string) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	client := &Client{hub: hub, name: name, conn: conn, send: make(chan []byte, 256)}
	client.hub.register <- client

	// Allow collection of memory referenced by the caller by doing all work in
	// new goroutines.
	go client.writePump()
	go client.readPump()
}

func (s *Server) extserveWs(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	name := c.Query("username")
	identifier := c.Param("hubidentifier")
	identifierint, err := strconv.ParseInt(identifier, 10, 64)
	var userhub *Hub
	userhub = nil
	if err != nil {
		log.Println(err)
		return
	}
	for hub := range s.hubs {
		if s.hubs[hub].identifier == identifierint {
			userhub = &s.hubs[hub]
			break
		}
	}
	if userhub == nil {
		log.Println("None existent room identifier : ", identifier)
		c.JSON(http.StatusNotFound, gin.H{"Error": "room not found"})
		return
	}
	client := &Client{hub: userhub, name: name, conn: conn, send: make(chan []byte, 256)}
	client.hub.register <- client

	// Allow collection of memory referenced by the caller by doing all work in
	// new goroutines.
	go client.writePump()
	go client.readPump()
}

func (s *Server) Startnew(c *gin.Context) {
	reader := rand.Reader
	min := big.NewInt(100000)
	maxint := big.NewInt(900000)
	name := c.Param("hubname")
	random, err := rand.Int(reader, maxint)
	if err != nil {
		log.Println("Internal server error! error : ", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal server error"})
		return
	}
	random = random.Add(random, min)
	hub := NewHub(name, random.Int64())

	s.hubs = append(s.hubs, *hub)

	go hub.run()

	c.JSON(http.StatusCreated, gin.H{"success": true, "identifier": random.Int64(), "Name": name, "meesage": "created a new hub with identifiers and name as given"})
}
