package config

import (
	"context"
	"log"
	"time"
    "encoding/json"
    "io/ioutil"
    "net/http"

	"github.com/ClickHouse/clickhouse-go/v2"
)

var Ctx = context.Background()
var ClickDB clickhouse.Conn

func InitClickHouse() {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "kopsusenak",
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	ClickDB = conn
	createTables()
	InitFuturesTokens()
}

func createTables() {
	// 1️⃣ market_stream
	query1 := `
	CREATE TABLE IF NOT EXISTS market_stream (
		symbol String,
		last_price String,
		volume_24h String,
		open_interest String,
		funding_rate String,
		ts UInt64
	) ENGINE = MergeTree()
	ORDER BY (symbol, ts);
	`
	if err := ClickDB.Exec(Ctx, query1); err != nil {
		log.Fatal("Create market_stream table error:", err)
	}

	// 2️⃣ tokens
	query2 := `
	CREATE TABLE IF NOT EXISTS tokens (
		id UInt64,
		token String,
		active UInt8 DEFAULT 0,
		created_at DateTime DEFAULT now()
	) ENGINE = MergeTree()
	ORDER BY id;
	`
	if err := ClickDB.Exec(Ctx, query2); err != nil {
		log.Fatal("Create tokens table error:", err)
	}

	log.Println("ClickHouse tables ready")
}

type BybitFuturesResp struct {
	Result []struct {
		Name         string `json:"name"`
		Status       string `json:"status"`
		QuoteCurrency string `json:"quote_currency"`
	} `json:"result"`
}

func InitFuturesTokens() {
	resp, err := http.Get("https://api.bybit.com/v2/public/symbols")
	if err != nil {
		log.Println("Bybit API error:", err)
		return
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	var apiResp BybitFuturesResp
	if err := json.Unmarshal(body, &apiResp); err != nil {
		log.Println("JSON parse error:", err)
		return
	}

	log.Printf("Bybit symbols fetched: %d", len(apiResp.Result))

	// Ambil existing token dari DB
	rows, err := ClickDB.Query(Ctx, `SELECT token FROM tokens`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	existing := make(map[string]bool)
	for rows.Next() {
		var t string
		rows.Scan(&t)
		existing[t] = true
	}

	// ambil max ID
	var maxID uint64
	row := ClickDB.QueryRow(Ctx, `SELECT MAX(id) FROM tokens`)
	row.Scan(&maxID)
	id := maxID + 1

	for _, s := range apiResp.Result {
		if s.QuoteCurrency != "USDT" || s.Status != "Trading" {
			continue
		}

		if _, ok := existing[s.Name]; ok {
			continue
		}

		active := uint8(0)
		if s.Name == "BTCUSDT" || s.Name == "ETHUSDT" {
			active = 1
		}

		err := ClickDB.Exec(Ctx,
			`INSERT INTO tokens (id, token, active, created_at) VALUES (?, ?, ?, ?)`,
			id, s.Name, active, time.Now(),
		)
		if err != nil {
			log.Fatal("Insert token error:", err)
		}
		id++
	}

	log.Println("Futures tokens initialized from Bybit")
}