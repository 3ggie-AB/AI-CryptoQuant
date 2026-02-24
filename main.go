package main

import (
	"fmt"
	"net/http"
	"time"

	"kopi_susu_gula_aren/config"
	"kopi_susu_gula_aren/stream"

	"github.com/gin-gonic/gin"
)

func main() {
	
	// Init
	config.InitRedis()
	config.InitClickHouse()

	// Start WebSocket di background
	go stream.StartStream()

	r := gin.Default()
	r.LoadHTMLGlob("templates/*")

	// SSE endpoint
	r.GET("/stream", func(c *gin.Context) {
		c.Writer.Header().Set("Content-Type", "text/event-stream")
		c.Writer.Header().Set("Cache-Control", "no-cache")
		c.Writer.Header().Set("Connection", "keep-alive")

		flusher, ok := c.Writer.(http.Flusher)
		if !ok {
			c.String(500, "Streaming unsupported")
			return
		}

		symbols := []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}

		for {
			for _, sym := range symbols {
				price, err := config.Redis.Get(config.Ctx, "ticker:"+sym).Result()
				if err != nil {
					continue
				}

				// Format SSE
				msg := fmt.Sprintf("data: {\"symbol\":\"%s\",\"price\":\"%s\"}\n\n", sym, price)
				c.Writer.Write([]byte(msg))
				flusher.Flush() // ❗ penting biar langsung terkirim
			}
			time.Sleep(time.Second) // refresh tiap detik
		}
	})

	// HTML frontend
	r.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.html", nil)
	})

	r.Run(":8080")
}