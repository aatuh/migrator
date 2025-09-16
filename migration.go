package migrator

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strconv"
)

// Executor is an interface that both *sql.DB and *sql.Tx implement.
type Executor interface {
	ExecContext(
		ctx context.Context, query string, args ...any,
	) (sql.Result, error)
}

// MigrationStep defines a step that can be executed in up/down mode.
type MigrationStep interface {
	ExecuteUp(ctx context.Context, exec Executor) error
	ExecuteDown(ctx context.Context, exec Executor) error
}

// MigrationSource defines the interface to load migrations.
type MigrationSource interface {
	LoadMigrations() ([]Migration, error)
}

// Migration holds a migration's version, name, and its up and down steps.
type Migration struct {
	Version   string // Name is usually derived from the filename.
	Name      string
	UpSteps   []MigrationStep
	DownSteps []MigrationStep
}

// NewMigration returns a new migration.
//
// Parameters:
//   - version: The version of the migration.
//   - name: The name of the migration.
//   - migrationName: The name of the migration.
//
// Returns:
//   - *Migration: A new migration.
func NewMigration(
	version string,
	name string,
) *Migration {
	return &Migration{
		Version: version,
		Name:    name,
	}
}

// WithVersion returns a new Migration with the given version.
//
// Parameters:
//   - version: The version of the migration.
//
// Returns:
//   - *Migration: A new migration.
func (m *Migration) WithVersion(version string) *Migration {
	new := *m
	new.Version = version
	return &new
}

// WithName returns a new Migration with the given name.
//
// Parameters:
//   - name: The name of the migration.
//
// Returns:
//   - *Migration: A new migration.
func (m *Migration) WithName(name string) *Migration {
	new := *m
	new.Name = name
	return &new
}

// WithUpSteps returns a new Migration with the given up steps.
//
// Parameters:
//   - upSteps: The up steps to use.
//
// Returns:
//   - *Migration: A new migration.
func (m *Migration) WithUpSteps(upSteps []MigrationStep) *Migration {
	new := *m
	new.UpSteps = upSteps
	return &new
}

// WithDownSteps returns a new Migration with the given down steps.
//
// Parameters:
//   - downSteps: The down steps to use.
//
// Returns:
//   - *Migration: A new migration.
func (m *Migration) WithDownSteps(downSteps []MigrationStep) *Migration {
	new := *m
	new.DownSteps = downSteps
	return &new
}

// Migrator holds migrations from one or more sources and manages history.
type Migrator struct {
	Sources        []MigrationSource
	DB             *sql.DB
	HistoryTable   string
	HistoryManager HistoryManager
	MigrationName  string
	Transactional  bool
}

// NewMigrator returns a new Migrator instance.
// If historyManager is nil, it defaults to SQLiteHistoryManager.
//
// Parameters:
//   - db: A connection to the target database.
//   - historyTable: The name of the table used to record applied migrations.
//   - historyManager: Optional HistoryManager. Defaults to
//     SQLiteHistoryManager.
//   - migrationName: The name of the migration. It is used to distinguish
//     migrations between multiple systems.
//
// Returns:
//   - A pointer to a Migrator.
func NewMigrator(
	db *sql.DB,
	historyTable string,
	historyManager HistoryManager,
	migrationName string,
) *Migrator {
	if historyManager == nil {
		historyManager = SQLiteHistoryManager{}
	}
	return &Migrator{
		DB:             db,
		HistoryTable:   historyTable,
		HistoryManager: historyManager,
		MigrationName:  migrationName,
	}
}

// WithSources returns a new Migrator with the given sources.
//
// Parameters:
//   - sources: A slice of MigrationSource instances.
//
// Returns:
//   - *Migrator: A new Migrator instance.
func (m *Migrator) WithSources(sources []MigrationSource) *Migrator {
	new := *m
	new.Sources = sources
	return &new
}

// WithDB returns a new Migrator with the given database connection.
//
// Parameters:
//   - db: A database connection.
//
// Returns:
//   - *Migrator: A new Migrator instance.
func (m *Migrator) WithDB(db *sql.DB) *Migrator {
	new := *m
	new.DB = db
	return &new
}

// WithHistoryTable returns a new Migrator with the given history table name.
//
// Parameters:
//   - historyTable: The name of the history table.
//
// Returns:
//   - *Migrator: A new Migrator instance.
func (m *Migrator) WithHistoryTable(historyTable string) *Migrator {
	new := *m
	new.HistoryTable = historyTable
	return &new
}

// WithHistoryManager returns a new Migrator with the given HistoryManager.
//
// Parameters:
//   - historyManager: A HistoryManager instance.
//
// Returns:
//   - *Migrator: A new Migrator instance.
func (m *Migrator) WithHistoryManager(historyManager HistoryManager) *Migrator {
	new := *m
	new.HistoryManager = historyManager
	return &new
}

// WithMigrationName returns a new Migrator with the given migration name.
//
// Parameters:
//   - migrationName: The name of the migration.
//
// Returns:
//   - *Migrator: A new Migrator instance.
func (m *Migrator) WithMigrationName(migrationName string) *Migrator {
	new := *m
	new.MigrationName = migrationName
	return &new
}

// WithTransactional returns a new Migrator with the transactional flag set.
//
// Parameters:
//   - transactional: Whether to use transactions.
//
// Returns:
//   - *Migrator: A new Migrator instance.
func (m *Migrator) WithTransactional(transactional bool) *Migrator {
	new := *m
	new.Transactional = transactional
	return &new
}

// LoadAllMigrations loads and merges migrations from all sources and validates
// that each migration has at least one up step.
//
// Returns:
//   - A slice of loaded migrations.
//   - An error if any migration is missing up steps or loading fails.
func (m *Migrator) LoadAllMigrations() ([]Migration, error) {
	var all []Migration
	for _, src := range m.Sources {
		migs, err := src.LoadMigrations()
		if err != nil {
			return nil, err
		}
		all = append(all, migs...)
	}

	// Validate that every migration has at least one up step.
	for _, mig := range all {
		if len(mig.UpSteps) == 0 {
			return nil, fmt.Errorf(
				"migration %s (%s) has no up steps defined",
				mig.Version,
				mig.Name,
			)
		}
	}

	// Sort migrations by version (assumes numeric versions).
	sort.Slice(all, func(i, j int) bool {
		vi, _ := strconv.Atoi(all[i].Version)
		vj, _ := strconv.Atoi(all[j].Version)
		return vi < vj
	})
	log.Printf("Total loaded migrations: %d", len(all))
	return all, nil
}

// MigrateUp applies pending migrations up to a target version.
// If target is empty, all pending migrations are applied.
//
// Parameters:
//   - ctx: Context to use for database operations.
//   - target: The target migration version to stop at (empty means all).
//
// Returns:
//   - An error if any migration fails.
func (m *Migrator) MigrateUp(ctx context.Context, target string) error {
	log.Println("Starting MigrateUp")

	err := m.ensureHistoryTable(ctx)
	if err != nil {
		return err
	}

	all, applied, err := m.getAllAndAppliedMigrations(ctx)
	if err != nil {
		return err
	}

	count, err := m.runMigrationsIfTransactional(
		ctx,
		func(exec Executor) (int, error) {
			return m.applyMigrations(ctx, exec, all, applied, target)
		},
	)
	if err != nil {
		return err
	}

	log.Printf("MigrateUp complete. Total migrations applied: %d", count)
	return nil
}

// MigrateDown rolls back applied migrations down to a target version.
// If target is empty, all applied migrations are rolled back.
//
// Parameters:
//   - ctx: Context to use for database operations.
//   - target: The migration version at which to stop rolling back
//     (empty means rollback all).
//
// Returns:
//   - An error if any rollback step fails.
func (m *Migrator) MigrateDown(ctx context.Context, target string) error {
	log.Println("Starting MigrateDown")

	all, applied, err := m.getAllAndAppliedMigrations(ctx)
	if err != nil {
		return err
	}

	// Sort migrations in reverse order by version.
	sort.Slice(all, func(i, j int) bool {
		vi, _ := strconv.Atoi(all[i].Version)
		vj, _ := strconv.Atoi(all[j].Version)
		return vi > vj
	})

	count, err := m.runMigrationsIfTransactional(
		ctx,
		func(exec Executor) (int, error) {
			return m.rollbackMigrations(ctx, exec, all, applied, target)
		},
	)
	if err != nil {
		return err
	}

	log.Printf("MigrateDown complete. Total migrations rolled back: %d", count)
	return nil
}

// ensureHistoryTable ensures the history table exists.
func (m *Migrator) ensureHistoryTable(ctx context.Context) error {
	// Ensure history table exists.
	log.Println("Starting MigrateUp")
	if err := m.HistoryManager.EnsureHistoryTable(
		ctx, m.DB, m.HistoryTable,
	); err != nil {
		log.Printf("Error ensuring history table %s: %v", m.HistoryTable, err)
		return err
	}
	log.Printf("History table %s ensured", m.HistoryTable)
	return nil
}

// getAllAndAppliedMigrations loads all migrations and their applied status.
func (m *Migrator) getAllAndAppliedMigrations(
	ctx context.Context,
) ([]Migration, map[string]bool, error) {
	// Load all migrations.
	all, err := m.LoadAllMigrations()
	if err != nil {
		log.Printf("Error loading migrations: %v", err)
		return nil, nil, err
	}

	// Get a list of migrations that have been applied.
	applied, err := m.HistoryManager.AppliedMigrations(
		ctx, m.DB, m.HistoryTable, m.MigrationName,
	)
	if err != nil {
		log.Printf("Error retrieving applied migrations: %v", err)
		return nil, nil, err
	}
	log.Printf("Previously applied migrations count: %d", len(applied))

	return all, applied, nil
}

// runMigrationsIfTransactional applies or rolls back migrations.
func (m *Migrator) runMigrationsIfTransactional(
	ctx context.Context, migrationFn func(exec Executor) (int, error),
) (int, error) {
	// Begin transaction.
	exec, tx, err := m.getTransactionIfTransactional(ctx)
	if err != nil {
		return 0, err
	}

	// Run migrations.
	rollbackCount, err := migrationFn(exec)
	if err != nil {
		return 0, m.rollbackIfTransactional(tx, err)
	}

	// Commit the transaction.
	err = m.commitIfTransactional(tx)
	if err != nil {
		return 0, err
	}

	return rollbackCount, nil
}

// getTransactionIfTransactional creates a transaction if transactional is true.
func (m *Migrator) getTransactionIfTransactional(
	ctx context.Context,
) (Executor, *sql.Tx, error) {
	if m.Transactional {
		tx, err := m.DB.BeginTx(ctx, nil)
		if err != nil {
			return nil, nil, err
		}
		return tx, tx, nil
	} else {
		return m.DB, nil, nil
	}
}

// rollbackIfTransactional rolls back the transaction if it exists.
func (m *Migrator) rollbackIfTransactional(tx *sql.Tx, err error) error {
	if m.Transactional {
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Printf("Error rolling back transaction: %v", rbErr)
			return fmt.Errorf(
				"rollbackIfTransactional: error processing migration: %v, "+
					"also error rolling back transaction: %v",
				err,
				rbErr,
			)
		}
		log.Printf("Error processing migration: %v", err)
	}

	return err
}

// commitIfTransactional commits the transaction if it exists.
func (m *Migrator) commitIfTransactional(tx *sql.Tx) error {
	if m.Transactional {
		if err := tx.Commit(); err != nil {
			log.Printf("Error committing transaction: %v", err)
			return err
		}
	}
	return nil
}

// applyMigrations applies migrations a slice of migrations to the database.
func (m *Migrator) applyMigrations(
	ctx context.Context,
	exec Executor,
	all []Migration,
	applied map[string]bool,
	target string,
) (int, error) {
	count := 0
	for _, mig := range all {
		if applied[mig.Version] {
			log.Printf("Skip applied migration %s: %s", mig.Version, mig.Name)
			continue
		}
		if m.isTargetReached(target, mig, "up") {
			break
		}
		count++
		if err := m.executeAndRecordMigration(ctx, exec, mig); err != nil {
			return 0, err
		}
	}

	return count, nil
}

// rollbackMigrations rolls back a slice of migrations from the database.
func (m *Migrator) rollbackMigrations(
	ctx context.Context,
	exec Executor,
	all []Migration,
	applied map[string]bool,
	target string,
) (int, error) {
	count := 0
	for _, mig := range all {
		if !applied[mig.Version] {
			log.Printf("Skip unapplied migration %s: %s", mig.Version, mig.Name)
			continue
		}
		if m.isTargetReached(target, mig, "down") {
			break
		}
		count++
		if err := m.rollbackAndRemoveMigration(ctx, exec, mig); err != nil {
			return 0, err
		}
	}

	return count, nil
}

// isTargetReached returns true if the target migration has been reached.
func (m *Migrator) isTargetReached(
	target string, mig Migration, direction string,
) bool {
	if target != "" {
		t, _ := strconv.Atoi(target)
		v, _ := strconv.Atoi(mig.Version)

		if (direction == "up" && v > t) || (direction == "down" && v < t) {
			log.Printf(
				"Reached target version. Stopping at migration %s",
				mig.Version,
			)
			return true
		}
	}
	return false
}

// executeAndRecordMigration executes a migration and records it.
func (m *Migrator) executeAndRecordMigration(
	ctx context.Context, exec Executor, mig Migration,
) error {
	log.Printf("Beginning migration %s: %s", mig.Version, mig.Name)

	// Execute the migration.
	if err := executeSteps(
		ctx, exec, mig.UpSteps, mig.Version, "up",
	); err != nil {
		return err
	}

	// Record the applied migration.
	if err := m.HistoryManager.RecordMigration(
		ctx, exec, m.HistoryTable, mig, m.MigrationName,
	); err != nil {
		log.Printf("Error recording migration %s: %v", mig.Version, err)
		return err
	}

	log.Printf("Migration %s applied successfully", mig.Version)
	return nil
}

// rollbackAndRemoveMigration rolls back a migration and removes its record.
func (m *Migrator) rollbackAndRemoveMigration(
	ctx context.Context, exec Executor, mig Migration,
) error {
	log.Printf("Rolling back migration %s: %s", mig.Version, mig.Name)

	if err := executeSteps(
		ctx, exec, mig.DownSteps, mig.Version, "down",
	); err != nil {
		return err
	}
	if err := m.HistoryManager.RemoveMigration(
		ctx, exec, m.HistoryTable, mig, m.MigrationName,
	); err != nil {
		log.Printf(
			"Error removing migration record for %s: %v", mig.Version, err,
		)
		return err
	}

	log.Printf("Migration %s rolled back successfully", mig.Version)
	return nil
}

// executeSteps executes a slice of migration steps in the given direction.
func executeSteps(
	ctx context.Context,
	exec Executor,
	steps []MigrationStep,
	migVersion string,
	direction string,
) error {
	for idx, step := range steps {
		log.Printf(
			"Executing %s step %d for migration %s",
			direction,
			idx+1,
			migVersion,
		)
		var err error
		if direction == "up" {
			err = step.ExecuteUp(ctx, exec)
		} else {
			err = step.ExecuteDown(ctx, exec)
		}
		if err != nil {
			return err
		}
		log.Printf(
			"Successfully executed %s step %d for migration %s",
			direction,
			idx+1,
			migVersion,
		)
	}
	log.Printf(
		"Successfully executed all %s steps for migration %s",
		direction,
		migVersion,
	)
	return nil
}
