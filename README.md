# migrator

Simple, flexible DB migration runner with pluggable sources (dir, file,
vars), SQL and hook steps, history tracking (SQLite/MySQL), and optional
transactions.

## Install

```go
import "github.com/aatuh/migrator"
```

## Quick start

```go
// Define sources (from a dir of sql files like 001_init_up.sql, 001_init_down.sql)
src := migrator.NewDirMigrationSource("./migrations")

m := migrator.NewMigrator(db, "schema_migrations", nil /* SQLite by default */, "app").
  WithSources([]migrator.MigrationSource{src}).
  WithTransactional(true)

if err := m.MigrateUp(ctx, ""); err != nil { /* handle */ }
```

### File/var sources

```go
f := migrator.NewFileMigrationSource("./001_init.sql")
v := migrator.NewVarMigrationSource("002", "add_users", "CREATE TABLE users(...)", "DROP TABLE users")
```

### Hooks

```go
pre := func(ctx context.Context, exec migrator.Executor, path string) error { return nil }
post := func(ctx context.Context, exec migrator.Executor, path string) error { return nil }

src = src.WithFilenameParser(nil).WithAllowedExts([]string{".sql"})
src.ResolveHooks = func(filename string) (migrator.FileHookFn, migrator.FileHookFn) { return pre, post }
```

## Notes

- Filenames parsed as `VERSION_name_up.sql` / `VERSION_name_down.sql` by default.
- Versions are sorted numerically; ensure zero‑padded numbers if needed.
- History managers: SQLite (default) and MySQL; provide your own by
  implementing `HistoryManager`.
- `MigrateUp(target)`/`MigrateDown(target)` stop at a version when set;
  empty `target` applies/rolls back all.
