package db_fd_model

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rsmrtk/db-fd-model/m_expense"
	"github.com/rsmrtk/db-fd-model/m_income"
	"github.com/rsmrtk/db-fd-model/m_options"
	"github.com/rsmrtk/smartlg/logger"
)

type Model struct {
	DB *pgxpool.Pool
	//
	Expense *m_expense.Facade
	Income  *m_income.Facade
}

type Options struct {
	PostgresURL string // Format: "postgres://user:password@host:port/dbname?sslmode=disable"
	Log         *logger.Logger

	// Optional connection pool settings
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

func New(ctx context.Context, o *Options) (*Model, error) {
	// Parse config for pgxpool
	config, err := pgxpool.ParseConfig(o.PostgresURL)
	if err != nil {
		o.Log.Error("Failed to parse PostgreSQL connection string", logger.H{"error": err})
		return nil, fmt.Errorf("failed to parse PostgreSQL connection string: %w", err)
	}

	// Configure connection pool
	if o.MaxOpenConns > 0 {
		config.MaxConns = int32(o.MaxOpenConns)
	} else {
		config.MaxConns = 25 // Default
	}

	if o.MaxIdleConns > 0 {
		config.MinConns = int32(o.MaxIdleConns)
	} else {
		config.MinConns = 5 // Default
	}

	if o.ConnMaxLifetime > 0 {
		config.MaxConnLifetime = o.ConnMaxLifetime
	} else {
		config.MaxConnLifetime = 5 * time.Minute // Default
	}

	if o.ConnMaxIdleTime > 0 {
		config.MaxConnIdleTime = o.ConnMaxIdleTime
	} else {
		config.MaxConnIdleTime = 90 * time.Second // Default
	}

	// Create connection pool
	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		o.Log.Error("Failed to create PostgreSQL connection pool", logger.H{"error": err})
		return nil, fmt.Errorf("failed to create PostgreSQL connection pool: %w", err)
	}

	// Test connection
	if err := db.Ping(ctx); err != nil {
		o.Log.Error("[PKG DB] Failed to ping PostgreSQL.", map[string]any{
			"error": err,
		})
		db.Close()
		return nil, fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	opt := &m_options.Options{
		Log: o.Log,
		DB:  db,
	}

	return &Model{
		DB: db,
		//
		Expense: m_expense.New(opt),
		Income:  m_income.New(opt),
	}, nil
}

// Close closes the database connection pool
func (m *Model) Close() {
	if m.DB != nil {
		m.DB.Close()
	}
}
