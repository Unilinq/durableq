// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"embed"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

//go:embed migration/*.sql
var migrationFS embed.FS

// Migration is one versioned schema change, parsed from a goose-format file.
type Migration struct {
	Version int
	Name    string
	Up      string
	Down    string
}

// Migrations returns every embedded migration in version order.
func Migrations() ([]Migration, error) {
	entries, err := migrationFS.ReadDir("migration")
	if err != nil {
		return nil, err
	}
	var out []Migration
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		body, err := migrationFS.ReadFile(path.Join("migration", e.Name()))
		if err != nil {
			return nil, err
		}
		m, err := parseMigration(e.Name(), string(body))
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

func parseMigration(filename, body string) (Migration, error) {
	base := strings.TrimSuffix(filename, ".sql")
	numStr, name, ok := strings.Cut(base, "_")
	if !ok {
		return Migration{}, fmt.Errorf("migration %q: expected <version>_<name>.sql", filename)
	}
	version, err := strconv.Atoi(numStr)
	if err != nil {
		return Migration{}, fmt.Errorf("migration %q: bad version: %w", filename, err)
	}

	upIdx := strings.Index(body, "-- +goose Up")
	downIdx := strings.Index(body, "-- +goose Down")
	if upIdx < 0 || downIdx < 0 || downIdx < upIdx {
		return Migration{}, fmt.Errorf("migration %q: needs a +goose Up section followed by a +goose Down section", filename)
	}
	up := strings.TrimSpace(body[upIdx+len("-- +goose Up") : downIdx])
	down := strings.TrimSpace(body[downIdx+len("-- +goose Down"):])
	if up == "" || down == "" {
		return Migration{}, fmt.Errorf("migration %q: both sections must be non-empty; a migration that cannot be reversed is not acceptable", filename)
	}
	return Migration{Version: version, Name: name, Up: up, Down: down}, nil
}

// QuoteIdent quotes a SQL identifier. Exported because callers building their
// own schema names need the same escaping the adapter uses.
func QuoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

const migrationTable = "durableq_migrations"

// CreateSchema creates schema if it does not already exist.
func CreateSchema(ctx context.Context, conn *pgx.Conn, schema string) error {
	_, err := conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+QuoteIdent(schema))
	return err
}

// DropSchema removes schema and everything in it.
func DropSchema(ctx context.Context, conn *pgx.Conn, schema string) error {
	_, err := conn.Exec(ctx, "DROP SCHEMA IF EXISTS "+QuoteIdent(schema)+" CASCADE")
	return err
}

// MigrateUp applies every pending migration to schema, creating the schema if
// needed. It is safe to call repeatedly.
func MigrateUp(ctx context.Context, conn *pgx.Conn, schema string) error {
	if err := CreateSchema(ctx, conn, schema); err != nil {
		return err
	}
	if err := withSearchPath(ctx, conn, schema, func() error {
		_, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+migrationTable+` (
			version     integer     PRIMARY KEY,
			name        text        NOT NULL,
			applied_at  timestamptz NOT NULL DEFAULT now()
		)`)
		return err
	}); err != nil {
		return err
	}

	migrations, err := Migrations()
	if err != nil {
		return err
	}
	applied, err := appliedVersions(ctx, conn, schema)
	if err != nil {
		return err
	}
	for _, m := range migrations {
		if applied[m.Version] {
			continue
		}
		if err := runInSchemaTx(ctx, conn, schema, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.Up); err != nil {
				return fmt.Errorf("migration %d (%s) up: %w", m.Version, m.Name, err)
			}
			_, err := tx.Exec(ctx, `INSERT INTO `+migrationTable+` (version, name) VALUES ($1, $2)`, m.Version, m.Name)
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

// MigrateDown rolls back migrations above target, newest first. Target 0 rolls
// everything back.
func MigrateDown(ctx context.Context, conn *pgx.Conn, schema string, target int) error {
	migrations, err := Migrations()
	if err != nil {
		return err
	}
	applied, err := appliedVersions(ctx, conn, schema)
	if err != nil {
		return err
	}
	for i := len(migrations) - 1; i >= 0; i-- {
		m := migrations[i]
		if m.Version <= target || !applied[m.Version] {
			continue
		}
		if err := runInSchemaTx(ctx, conn, schema, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.Down); err != nil {
				return fmt.Errorf("migration %d (%s) down: %w", m.Version, m.Name, err)
			}
			_, err := tx.Exec(ctx, `DELETE FROM `+migrationTable+` WHERE version = $1`, m.Version)
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

// AppliedVersion returns the highest migration version applied to schema, or 0.
func AppliedVersion(ctx context.Context, conn *pgx.Conn, schema string) (int, error) {
	applied, err := appliedVersions(ctx, conn, schema)
	if err != nil {
		return 0, err
	}
	max := 0
	for v := range applied {
		if v > max {
			max = v
		}
	}
	return max, nil
}

func appliedVersions(ctx context.Context, conn *pgx.Conn, schema string) (map[int]bool, error) {
	out := map[int]bool{}
	var exists bool
	err := conn.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)`,
		schema, migrationTable).Scan(&exists)
	if err != nil {
		return nil, err
	}
	if !exists {
		return out, nil
	}
	rows, err := conn.Query(ctx, `SELECT version FROM `+QuoteIdent(schema)+`.`+migrationTable)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out[v] = true
	}
	return out, rows.Err()
}

func withSearchPath(ctx context.Context, conn *pgx.Conn, schema string, fn func() error) error {
	if _, err := conn.Exec(ctx, "SET search_path TO "+QuoteIdent(schema)); err != nil {
		return err
	}
	defer func() { _, _ = conn.Exec(ctx, "SET search_path TO DEFAULT") }()
	return fn()
}

func runInSchemaTx(ctx context.Context, conn *pgx.Conn, schema string, fn func(pgx.Tx) error) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL search_path TO "+QuoteIdent(schema)); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
