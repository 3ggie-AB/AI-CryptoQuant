package config

import (
	"context"
	"log"
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

	createTable()
}

func createTable() {

	query := `
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

	err := ClickDB.Exec(Ctx, query)

	if err != nil {
		log.Fatal("Create table error:", err)
	}

	log.Println("ClickHouse table ready")
}