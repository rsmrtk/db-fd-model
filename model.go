package db_fd_model

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rsmrtk/db-fd-model/m_income"
	"github.com/rsmrtk/db-fd-model/m_options"
	"github.com/rsmrtk/smartlg/logger"
)

type Model struct {
	DB *pgxpool.Pool
	//
	Income *m_income.Facade
}

type Options struct {
	PostgresURL string // PostgreSQL connection string (e.g., "postgres://user:pass@localhost:5432/dbname")
	Log         *logger.Logger
}

func New(ctx context.Context, o *Options) (*Model, error) {
	// Parse the connection string and create a pool config
	config, err := pgxpool.ParseConfig(o.PostgresURL)
	if err != nil {
		o.Log.Error("Failed to parse PostgreSQL connection string", logger.H{"error": err})
		return nil, fmt.Errorf("failed to parse PostgreSQL connection string: %w", err)
	}

	// Configure connection pool settings
	config.MinConns = 10
	config.MaxConns = 100

	// Create the connection pool
	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		o.Log.Error("Failed to create PostgreSQL connection pool", logger.H{"error": err})
		return nil, fmt.Errorf("failed to create PostgreSQL connection pool: %w", err)
	}

	// Test the connection
	if err := ping(ctx, db); err != nil {
		o.Log.Error("[PKG DB] Failed to ping PostgreSQL.", map[string]any{
			"error": err,
		})
		return nil, fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	opt := &m_options.Options{
		Log: o.Log,
		DB:  db,
	}

	return &Model{
		DB: db,
		//
		Income: m_income.New(opt),
	}, nil
}

func ping(ctx context.Context, db *pgxpool.Pool) error {
	var testResult int
	err := db.QueryRow(ctx, "SELECT 1").Scan(&testResult)
	if err != nil {
		return fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}
	if testResult != 1 {
		return fmt.Errorf("unexpected ping result: %d", testResult)
	}
	return nil
}

// Close closes the database connection pool
func (m *Model) Close() {
	if m.DB != nil {
		m.DB.Close()
	}
}