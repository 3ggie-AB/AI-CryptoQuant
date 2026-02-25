package models

import (
	"context"
	"kopi_susu_gula_aren/config"
)

type Token struct {
	ID        uint64
	Token     string
	Active    uint8
	CreatedAt string
}

// Ambil semua token
func GetAllTokens(all bool) ([]Token, error) {
	var query string
	if all {
		query = `SELECT id, token, active, created_at FROM tokens ORDER BY id`
	} else {
		query = `SELECT id, token, active, created_at FROM tokens WHERE active = 1 ORDER BY id`
	}

	rows, err := config.ClickDB.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tokens []Token
	for rows.Next() {
		var t Token
		if err := rows.Scan(&t.ID, &t.Token, &t.Active, &t.CreatedAt); err != nil {
			continue
		}
		tokens = append(tokens, t)
	}
	return tokens, nil
}

// Toggle active token
func ToggleToken(id uint64) error {
	err := config.ClickDB.Exec(context.Background(),
		`UPDATE tokens SET active = 1 - active WHERE id = ?`, id)
	return err
}