package migrator

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// HistoryManager defines methods to manage migration history.
type HistoryManager interface {
	// EnsureHistoryTable creates the history table if it does not exist.
	EnsureHistoryTable(ctx context.Context, db *sql.DB, tableName string) error
	// RecordMigration inserts a record for the applied migration.
	RecordMigration(
		ctx context.Context,
		exec Executor,
		tableName string,
		mig Migration,
		migrationName string,
	) error
	// RemoveMigration deletes the record for the given migration.
	RemoveMigration(
		ctx context.Context,
		exec Executor,
		tableName string,
		mig Migration,
		migrationName string,
	) error
	// AppliedMigrations retrieves applied migrations as a map.
	AppliedMigrations(
		ctx context.Context, db *sql.DB, tableName string, migrationName string,
	) (map[string]bool, error)
}

// MySQLHistoryManager implements HistoryManager for MySQL.
type MySQLHistoryManager struct{}

// NewMySQLHistoryManager returns a new MySQLHistoryManager.
//
// Returns:
//   - *MySQLHistoryManager: A new MySQLHistoryManager instance.
func NewMySQLHistoryManager() *MySQLHistoryManager {
	return &MySQLHistoryManager{}
}

// EnsureHistoryTable creates the history table in MySQL.
//
// Parameters:
//   - ctx: Context to use.
//   - db: The database connection.
//   - tableName: The name of the history table.
//
// Returns:
//   - error: An error if the table creation fails.
func (m MySQLHistoryManager) EnsureHistoryTable(
	ctx context.Context, db *sql.DB, tableName string,
) error {
	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
		version VARCHAR(50) PRIMARY KEY,
		name VARCHAR(255),
		migration_name VARCHAR(255),
		applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)`,
		tableName,
	)
	_, err := db.ExecContext(ctx, query)
	return err
}

// RecordMigration inserts an applied migration record in MySQL.
//
// Parameters:
//   - ctx: Context to use.
//   - exec: The executor to use.
//   - tableName: The name of the history table.
//   - mig: The migration to record.
//   - migrationName: The name of the migration.
//
// Returns:
//   - error: An error if the record insertion fails.
func (m MySQLHistoryManager) RecordMigration(
	ctx context.Context,
	exec Executor,
	tableName string,
	mig Migration,
	migrationName string,
) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (version, name, migration_name, applied_at) VALUES (?, ?, ?, ?)`,
		tableName,
	)
	_, err := exec.ExecContext(
		ctx, query, mig.Version, mig.Name, migrationName, time.Now().UTC(),
	)
	return err
}

// RemoveMigration deletes the migration record in MySQL.
//
// Parameters:
//   - ctx: Context to use.
//   - exec: The executor to use.
//   - tableName: The name of the history table.
//   - mig: The migration to remove.
//   - migrationName: The name of the migration.
//
// Returns:
//   - error: An error if the record deletion fails.
func (m MySQLHistoryManager) RemoveMigration(
	ctx context.Context,
	exec Executor,
	tableName string,
	mig Migration,
	migrationName string,
) error {
	query := fmt.Sprintf(
		`DELETE FROM %s WHERE version = ? AND migration_name = ?`,
		tableName,
	)
	_, err := exec.ExecContext(ctx, query, mig.Version, migrationName)
	return err
}

// AppliedMigrations retrieves applied migrations from MySQL.
//
// Parameters:
//   - ctx: Context to use.
//   - db: The database connection.
//   - tableName: The name of the history table.
//   - migrationName: The name of the migration.
//
// Returns:
//   - map[string]bool: A map of applied migrations.
//   - error: An error if the query fails.
func (m MySQLHistoryManager) AppliedMigrations(
	ctx context.Context, db *sql.DB, tableName string, migrationName string,
) (map[string]bool, error) {
	migs := make(map[string]bool)
	query := fmt.Sprintf(
		`SELECT version FROM %s AND migration_name = ?`, tableName,
	)
	rows, err := db.QueryContext(ctx, query, migrationName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ver string
		if err := rows.Scan(&ver); err != nil {
			return nil, err
		}
		migs[ver] = true
	}
	return migs, nil
}

// SQLiteHistoryManager implements HistoryManager for SQLite.
type SQLiteHistoryManager struct{}

// NewSQLiteHistoryManager returns a new SQLiteHistoryManager.
//
// Returns:
//   - *SQLiteHistoryManager: A new SQLiteHistoryManager instance.
func NewSQLiteHistoryManager() *SQLiteHistoryManager {
	return &SQLiteHistoryManager{}
}

// EnsureHistoryTable creates the history table in SQLite.
//
// Parameters:
//   - ctx: Context to use.
//   - db: The database connection.
//   - tableName: The name of the history table.
//
// Returns:
//   - error: An error if the table creation fails.
func (s SQLiteHistoryManager) EnsureHistoryTable(
	ctx context.Context, db *sql.DB, tableName string,
) error {
	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
		version TEXT PRIMARY KEY,
		name TEXT,
		migration_name TEXT,
		applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)`,
		tableName,
	)
	_, err := db.ExecContext(ctx, query)
	return err
}

// RecordMigration inserts an applied migration record in SQLite.
//
// Parameters:
//   - ctx: Context to use.
//   - exec: The executor to use.
//   - tableName: The name of the history table.
//   - mig: The migration to record.
//
// Returns:
//   - error: An error if the record insertion fails.
func (s SQLiteHistoryManager) RecordMigration(
	ctx context.Context,
	exec Executor,
	tableName string,
	mig Migration,
	migrationName string,
) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (version, name, migration_name, applied_at) VALUES (?, ?, ?, ?)`,
		tableName,
	)
	_, err := exec.ExecContext(
		ctx, query, mig.Version, mig.Name, migrationName, time.Now().UTC(),
	)
	return err
}

// RemoveMigration deletes the migration record in SQLite.
//
// Parameters:
//   - ctx: Context to use.
//   - exec: The executor to use.
//   - tableName: The name of the history table.
//   - mig: The migration to remove.
//   - migrationName: The name of the migration.
//
// Returns:
//   - error: An error if the record deletion fails.
func (s SQLiteHistoryManager) RemoveMigration(
	ctx context.Context,
	exec Executor,
	tableName string,
	mig Migration,
	migrationName string,
) error {
	query := fmt.Sprintf(
		`DELETE FROM %s WHERE version = ? AND migration_name = ?`,
		tableName,
	)
	_, err := exec.ExecContext(ctx, query, mig.Version, migrationName)
	return err
}

// AppliedMigrations retrieves applied migrations from SQLite.
//
// Parameters:
//   - ctx: Context to use.
//   - db: The database connection.
//   - tableName: The name of the history table.
//   - migrationName: The name of the migration.
//
// Returns:
//   - map[string]bool: A map of applied migrations.
//   - error: An error if the query fails.
func (s SQLiteHistoryManager) AppliedMigrations(
	ctx context.Context, db *sql.DB, tableName string, migrationName string,
) (map[string]bool, error) {
	migs := make(map[string]bool)
	query := fmt.Sprintf(
		`SELECT version FROM %s WHERE migration_name = ?`,
		tableName,
	)
	rows, err := db.QueryContext(ctx, query, migrationName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ver string
		if err := rows.Scan(&ver); err != nil {
			return nil, err
		}
		migs[ver] = true
	}
	return migs, nil
}
