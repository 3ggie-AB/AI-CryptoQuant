package main

import (
	"fmt"
	"net/http"
	"time"

	"kopi_susu_gula_aren/config"
	"kopi_susu_gula_aren/stream"
	"kopi_susu_gula_aren/models"

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

		for {
			// Ambil token aktif
			tokens, err := models.GetAllTokens(false) // false = cuma aktif
			if err != nil || len(tokens) == 0 {
				// fallback
				tokens = []models.Token{
					{Token: "BTCUSDT"},
					{Token: "ETHUSDT"},
				}
			}

			for _, t := range tokens {
				price, err := config.Redis.Get(config.Ctx, "ticker:"+t.Token).Result()
				if err != nil {
					continue
				}

				msg := fmt.Sprintf("data: {\"symbol\":\"%s\",\"price\":\"%s\"}\n\n", t.Token, price)
				c.Writer.Write([]byte(msg))
				flusher.Flush()
			}

			time.Sleep(time.Second)
		}
	})

	// HTML frontend
	r.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.html", nil)
	})

	r.GET("/tokens", func(c *gin.Context) {
		all := c.Query("all") == "true"
		tokens, err := models.GetAllTokens(all)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, tokens)
	})

	r.POST("/tokens/:id/toggle", func(c *gin.Context) {
		idStr := c.Param("id")
		var id uint64
		fmt.Sscan(idStr, &id)

		if err := models.ToggleToken(id); err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, gin.H{"status": "ok"})
	})

	r.Run(":8080")
}