package db_fd_model

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/rsmrtk/db-fd-model/m_expense"
	"github.com/rsmrtk/db-fd-model/m_income"
	"github.com/rsmrtk/db-fd-model/m_options"
	"github.com/rsmrtk/smartlg/logger"

	// PostgreSQL driver
	_ "github.com/lib/pq"
)

type Model struct {
	DB *sql.DB
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
	// Open PostgreSQL connection
	db, err := sql.Open("postgres", o.PostgresURL)
	if err != nil {
		o.Log.Error("Failed to open PostgreSQL connection", logger.H{"error": err})
		return nil, fmt.Errorf("failed to open PostgreSQL connection: %w", err)
	}

	// Configure connection pool
	if o.MaxOpenConns > 0 {
		db.SetMaxOpenConns(o.MaxOpenConns)
	} else {
		db.SetMaxOpenConns(25) // Default
	}

	if o.MaxIdleConns > 0 {
		db.SetMaxIdleConns(o.MaxIdleConns)
	} else {
		db.SetMaxIdleConns(5) // Default
	}

	if o.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(o.ConnMaxLifetime)
	} else {
		db.SetConnMaxLifetime(5 * time.Minute) // Default
	}

	if o.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(o.ConnMaxIdleTime)
	} else {
		db.SetConnMaxIdleTime(90 * time.Second) // Default
	}

	// Test connection
	if err := ping(ctx, db); err != nil {
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

func ping(ctx context.Context, db *sql.DB) error {
	// Use PingContext to verify connection
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	// Additional test query
	var testResult int
	err := db.QueryRowContext(ctx, "SELECT 1").Scan(&testResult)
	if err != nil {
		return fmt.Errorf("failed to execute test query: %w", err)
	}

	if testResult != 1 {
		return fmt.Errorf("unexpected test query result: %d", testResult)
	}

	return nil
}

// Close closes the database connection
func (m *Model) Close() error {
	if m.DB != nil {
		return m.DB.Close()
	}
	return nil
}

// BeginTx starts a new transaction
func (m *Model) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return m.DB.BeginTx(ctx, opts)
}

// Begin starts a new transaction with default options
func (m *Model) Begin() (*sql.Tx, error) {
	return m.DB.Begin()
}
