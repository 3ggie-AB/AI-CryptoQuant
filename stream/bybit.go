package stream

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"kopi_susu_gula_aren/config"
	"github.com/gorilla/websocket"
)

// lastTicker simpan state terakhir tiap symbol (untuk delta)
var lastTicker = make(map[string]map[string]string)

func StartStream() {
	rows, _ := config.ClickDB.Query(config.Ctx, `SELECT token FROM tokens WHERE active = 1`)
	var Symbols []string
	for rows.Next() {
		var sym string
		rows.Scan(&sym)
		Symbols = append(Symbols, sym)
	}
	conn, _, err := websocket.DefaultDialer.Dial(
		"wss://stream.bybit.com/v5/public/linear",
		nil,
	)
	if err != nil {
		log.Fatal("WebSocket dial error:", err)
	}

	fmt.Println("✅ Connected to Bybit WebSocket")

	// subscribe semua simbol
	sub := map[string]interface{}{
		"op":   "subscribe",
		"args": []string{},
	}
	for _, s := range Symbols {
		sub["args"] = append(sub["args"].([]string), "tickers."+s)
	}

	if err := conn.WriteJSON(sub); err != nil {
		log.Fatal("Subscribe error:", err)
	}
	fmt.Println("Subscribed to symbols:", Symbols)

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Println("ReadMessage error:", err)
			time.Sleep(time.Second)
			continue
		}

		// Print raw message full debug
		fmt.Println("Raw message:", string(msg))

		var data map[string]interface{}
		if err := json.Unmarshal(msg, &data); err != nil {
			fmt.Println("Unmarshal error:", err)
			continue
		}

		if data["data"] == nil {
			continue
		}

		// Bybit kadang ngirim array atau object
		var items []interface{}
		switch d := data["data"].(type) {
		case []interface{}:
			items = d
		case map[string]interface{}:
			items = []interface{}{d}
		default:
			continue
		}

		for _, item := range items {
			ticker, ok := item.(map[string]interface{})
			if !ok {
				continue
			}

			// ambil symbol
			symbol, _ := ticker["symbol"].(string)
			if symbol == "" {
				continue
			}

			if _, ok := lastTicker[symbol]; !ok {
				lastTicker[symbol] = make(map[string]string)
			}

			// ambil field, fallback ke last value kalau kosong
			lastPrice, ok := ticker["lastPrice"].(string)
			if !ok {
				lastPrice = lastTicker[symbol]["lastPrice"]
			}
			volume24h, ok := ticker["volume24h"].(string)
			if !ok {
				volume24h = lastTicker[symbol]["volume24h"]
			}
			openInterest, ok := ticker["openInterest"].(string)
			if !ok {
				openInterest = lastTicker[symbol]["openInterest"]
			}
			fundingRate, ok := ticker["fundingRate"].(string)
			if !ok {
				fundingRate = lastTicker[symbol]["fundingRate"]
			}

			// update lastTicker
			lastTicker[symbol]["lastPrice"] = lastPrice
			lastTicker[symbol]["volume24h"] = volume24h
			lastTicker[symbol]["openInterest"] = openInterest
			lastTicker[symbol]["fundingRate"] = fundingRate

			// Print debug
			fmt.Printf(
				"Ticker: %s | Price: %s | Volume24h: %s | OI: %s | Funding: %s\n",
				symbol, lastPrice, volume24h, openInterest, fundingRate,
			)

			// simpan ke Redis snapshot
			if lastPrice != "" {
				config.Redis.Set(
					config.Ctx,
					"ticker:"+symbol,
					lastPrice,
					10*time.Second,
				)
			}

			// simpan ke ClickHouse
			if lastPrice != "" && volume24h != "" {
				err := config.ClickDB.Exec(
					config.Ctx,
					`INSERT INTO market_stream
						(symbol, last_price, volume_24h, open_interest, funding_rate, ts)
					VALUES (?, ?, ?, ?, ?, ?)`,
					symbol,
					lastPrice,
					volume24h,
					openInterest,
					fundingRate,
					time.Now().UnixMilli(),
				)
				if err != nil {
					fmt.Println("ClickHouse insert error:", err)
				}
			}
		}
	}
}