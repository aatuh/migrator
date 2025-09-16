package migrator

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"slices"
)

// FileHookFn is a hook function that accepts a file path.
type FileHookFn func(ctx context.Context, exec Executor, filePath string) error

// HookFn is a generic hook function.
type HookFn func(ctx context.Context, exec Executor) error

// ParseFilenameFn defines a function to extract migration details from a
// file name. It returns the version, name, direction ("up" or "down"), and a
// boolean indicating if parsing succeeded.
type ParseFilenameFn func(filename string) (
	version string, name string, direction string, ok bool,
)

// defaultParseFilename is the built-in parser that expects file names in the
// format "001_create_table_up.sql" or "001_create_table_down.sql".
func defaultParseFilename(filename string) (string, string, string, bool) {
	base := strings.TrimSuffix(filename, path.Ext(filename))
	parts := strings.Split(base, "_")
	if len(parts) < 3 {
		return "", "", "", false
	}
	version := parts[0]
	direction := strings.ToLower(parts[len(parts)-1])
	if direction != "up" && direction != "down" {
		return "", "", "", false
	}
	name := strings.Join(parts[1:len(parts)-1], "_")
	return version, name, direction, true
}

// SQLMigrationStep executes a plain SQL statement.
type SQLMigrationStep struct {
	SQL string
}

// NewSQLMigrationStep returns a new SQLMigrationStep.
//
// Parameters:
//   - sql: The SQL statement to execute.
//
// Returns:
//   - *SQLMigrationStep: A new SQLMigrationStep.
func NewSQLMigrationStep(sql string) *SQLMigrationStep {
	return &SQLMigrationStep{
		SQL: sql,
	}
}

// WithSQL returns a new SQLMigrationStep with the given SQL statement.
//
// Parameters:
//   - sql: The SQL statement to execute.
//
// Returns:
//   - *SQLMigrationStep: A new SQLMigrationStep.
func (s *SQLMigrationStep) WithSQL(sql string) *SQLMigrationStep {
	new := *s
	new.SQL = sql
	return &new
}

// ExecuteUp executes the SQL query for upward migration.
//
// Parameters:
//   - ctx: Context to use.
//   - exec: The database connection.
//
// Returns:
//   - error: An error if the query execution fails.
func (s SQLMigrationStep) ExecuteUp(ctx context.Context, exec Executor) error {
	_, err := exec.ExecContext(ctx, s.SQL)
	return err
}

// ExecuteDown executes the SQL query for downward migration.
//
// Parameters:
//   - ctx: Context to use.
//   - exec: The database connection.
//
// Returns:
//   - error: An error if the query execution fails.
func (s SQLMigrationStep) ExecuteDown(
	ctx context.Context, exec Executor,
) error {
	_, err := exec.ExecContext(ctx, s.SQL)
	return err
}

// HookMigrationStep executes custom hook functions.
type HookMigrationStep struct {
	UpHook   HookFn
	DownHook HookFn
}

// NewHookMigrationStep returns a new HookMigrationStep with the given hooks.
//
// Returns:
//   - *MigrationStep: A new migration step.
func NewHookMigrationStep() *HookMigrationStep {
	return &HookMigrationStep{}
}

// WithUpHook returns a new HookMigrationStep with the given up hook.
//
// Parameters:
//   - upHook: The up hook to use.
//
// Returns:
//   - MigrationStep: A new migration step.
func (h *HookMigrationStep) WithUpHook(upHook HookFn) MigrationStep {
	new := h
	new.UpHook = upHook
	return new
}

// WithDownHook returns a new HookMigrationStep with the given down hook.
//
// Parameters:
//   - downHook: The down hook to use.
//
// Returns:
//   - MigrationStep: A new migration step.
func (h *HookMigrationStep) WithDownHook(downHook HookFn) MigrationStep {
	new := h
	new.DownHook = downHook
	return new
}

// ExecuteUp executes the custom up hook.
//
// Parameters:
//   - ctx: Context to use.
//   - exec: The database connection.
//
// Returns:
//   - error: An error if the hook execution fails.
func (h HookMigrationStep) ExecuteUp(ctx context.Context, exec Executor) error {
	if h.UpHook == nil {
		return fmt.Errorf("up hook not defined")
	}
	return h.UpHook(ctx, exec)
}

// ExecuteDown executes the custom down hook.
//
// Parameters:
//   - ctx: Context to use.
//     -exectx: The database connection.
//
// Returns:
//   - error: An error if the hook execution fails.
func (h HookMigrationStep) ExecuteDown(
	ctx context.Context, exec Executor,
) error {
	if h.DownHook == nil {
		return fmt.Errorf("down hook not defined")
	}
	return h.DownHook(ctx, exec)
}

// DirMigrationSource loads migrations from a directory. It supports
// optional hooks that can be explicitly tied to filenames.
type DirMigrationSource struct {
	Dir string
	// Optional filename parser, defaults to defaultParseFilename.
	FilenameParser ParseFilenameFn
	// Optional allowed extensions, defaults to .sql and .sqlite files.
	AllowedExts []string
	// Optional ResolveHooks returns hook functions for the given filename.
	ResolveHooks func(filename string) (preHook FileHookFn, postHook FileHookFn)
}

// NewDirMigrationSource creates a new DirMigrationSource for the given
// directory. The default parser and allowed extensions are used.
//
// Parameters:
//   - dir: The directory to load migrations from.
//
// Returns:
//   - *DirMigrationSource: A new DirMigrationSource instance.
func NewDirMigrationSource(dir string) *DirMigrationSource {
	return &DirMigrationSource{
		Dir:            dir,
		FilenameParser: defaultParseFilename,
		AllowedExts:    []string{".sql", ".sqlite"},
	}
}

// WithFilenameParser returns a new DirMigrationSource with the given parser.
//
// Parameters:
//   - parser: The ParseFilenameFn to use.
//
// Returns:
//   - *DirMigrationSource: A new DirMigrationSource instance.
func (d *DirMigrationSource) WithFilenameParser(
	parser ParseFilenameFn,
) *DirMigrationSource {
	new := *d
	new.FilenameParser = parser
	return &new
}

// WithAllowedExts returns a new DirMigrationSource with the given allowed
// extensions.
//
// Parameters:
//   - exts: A slice of allowed extensions.
//
// Returns:
//   - *DirMigrationSource: A new DirMigrationSource instance.
func (d *DirMigrationSource) WithAllowedExts(
	exts []string,
) *DirMigrationSource {
	new := *d
	new.AllowedExts = exts
	return &new
}

// LoadMigrations loads and merges migrations from the directory.
//
// Returns:
//   - []Migration: A slice containing the loaded migrations.
//   - error: An error if loading fails.
func (d *DirMigrationSource) LoadMigrations() ([]Migration, error) {
	entries, err := os.ReadDir(d.Dir)
	if err != nil {
		return nil, err
	}

	parser := d.FilenameParser
	if parser == nil {
		parser = defaultParseFilename
	}
	allowed := d.AllowedExts
	if allowed == nil {
		allowed = []string{".sql", ".sqlite"}
	}

	mMap := make(map[string]*Migration)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		ext := strings.ToLower(path.Ext(name))
		if !slices.Contains(allowed, ext) {
			log.Printf("Skipping file %s due to unsupported ext %s", name, ext)
			continue
		}
		version, migName, direction, ok := parser(name)
		if !ok {
			log.Printf("Skipping file %s due to parsing failure", name)
			continue
		}

		mig, exists := mMap[version]
		if !exists {
			mig = NewMigration(version, migName)
			mMap[version] = mig
		}

		fullPath := path.Join(d.Dir, name)
		content, err := os.ReadFile(fullPath)
		if err != nil {
			return nil, err
		}

		var preHook, postHook FileHookFn
		if d.ResolveHooks != nil {
			preHook, postHook = d.ResolveHooks(name)
		}

		switch direction {
		case "up":
			if preHook != nil {
				preStep := NewHookMigrationStep().WithUpHook(
					func(ctx context.Context, exec Executor) error {
						return preHook(ctx, exec, fullPath)
					},
				)
				mig.UpSteps = append(mig.UpSteps, preStep)
			}
			mig.UpSteps = append(
				mig.UpSteps,
				NewSQLMigrationStep(string(content)),
			)
			if postHook != nil {
				postStep := NewHookMigrationStep().WithUpHook(
					func(ctx context.Context, exec Executor) error {
						return postHook(ctx, exec, fullPath)
					},
				)
				mig.UpSteps = append(mig.UpSteps, postStep)
			}
		case "down":
			if preHook != nil {
				preStep := NewHookMigrationStep().WithDownHook(
					func(ctx context.Context, exec Executor) error {
						return preHook(ctx, exec, fullPath)
					},
				)
				mig.DownSteps = append(mig.DownSteps, preStep)
			}
			mig.DownSteps = append(
				mig.DownSteps,
				NewSQLMigrationStep(string(content)),
			)
			if postHook != nil {
				postStep := NewHookMigrationStep().WithDownHook(
					func(ctx context.Context, exec Executor) error {
						return postHook(ctx, exec, fullPath)
					},
				)
				mig.DownSteps = append(mig.DownSteps, postStep)
			}
		default:
			return nil, fmt.Errorf("invalid direction: %s", direction)
		}
	}

	var migrations []Migration
	for _, mig := range mMap {
		migrations = append(migrations, *mig)
	}
	sort.Slice(migrations, func(i, j int) bool {
		vi, _ := strconv.Atoi(migrations[i].Version)
		vj, _ := strconv.Atoi(migrations[j].Version)
		return vi < vj
	})
	log.Printf("Loaded %d migrations from directory %s", len(migrations), d.Dir)
	return migrations, nil
}

// FileMigrationSource loads a single migration file and supports optional hooks.
type FileMigrationSource struct {
	FilePath string
	// Optional filename parser, defaults to defaultParseFilename
	FilenameParser ParseFilenameFn
	// Optional pre-hook.
	PreHook FileHookFn
	// Optional post-hook.
	PostHook FileHookFn
}

// NewFileMigrationSource returns a new FileMigrationSource.
//
// Returns:
//   - *FileMigrationSource: A new FileMigrationSource instance.
func NewFileMigrationSource(filePath string) *FileMigrationSource {
	return &FileMigrationSource{
		FilePath: filePath,
	}
}

// WithFilenameParser returns a new FileMigrationSource with the given parser.
//
// Parameters:
//   - parser: The ParseFilenameFn to use.
//
// Returns:
//   - *FileMigrationSource: A new FileMigrationSource instance.
func (f *FileMigrationSource) WithFilenameParser(
	parser ParseFilenameFn,
) *FileMigrationSource {
	new := *f
	new.FilenameParser = parser
	return &new
}

// WithPreHook returns a new FileMigrationSource with the given pre-hook.
//
// Parameters:
//   - preHook: The pre-hook to use.
//
// Returns:
//   - *FileMigrationSource: A new FileMigrationSource with the given pre-hook.
func (f *FileMigrationSource) WithPreHook(
	preHook FileHookFn,
) *FileMigrationSource {
	new := *f
	new.PreHook = preHook
	return &new
}

// WithPostHook returns a new FileMigrationSource with the given post-hook.
//
// Parameters:
//   - postHook: The post-hook to use.
//
// Returns:
//   - *FileMigrationSource: A new FileMigrationSource with the given post-hook.
func (f *FileMigrationSource) WithPostHook(
	postHook FileHookFn,
) *FileMigrationSource {
	new := *f
	new.PostHook = postHook
	return &new
}

// LoadMigrations loads the migration from the file.
//
// Returns:
//   - []Migration: A slice containing the loaded migration.
//   - error: An error if loading fails.
func (f *FileMigrationSource) LoadMigrations() ([]Migration, error) {
	content, err := os.ReadFile(f.FilePath)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(string(content), "-- DOWN")
	upSQL := strings.TrimSpace(parts[0])
	downSQL := ""
	if len(parts) > 1 {
		downSQL = strings.TrimSpace(parts[1])
	}
	var version, name string
	parser := f.FilenameParser
	if parser == nil {
		version = strings.TrimSuffix(path.Base(f.FilePath),
			path.Ext(f.FilePath))
		name = version
	} else {
		v, n, _, ok := parser(path.Base(f.FilePath))
		if !ok {
			version = strings.TrimSuffix(path.Base(f.FilePath),
				path.Ext(f.FilePath))
			name = version
		} else {
			version = v
			name = n
		}
	}
	mig := NewMigration(version, name)
	if f.PreHook != nil {
		preStep := NewHookMigrationStep().WithUpHook(
			func(ctx context.Context, exec Executor) error {
				return f.PreHook(ctx, exec, f.FilePath)
			},
		)
		mig.UpSteps = append(mig.UpSteps, preStep)
	}
	mig.UpSteps = append(mig.UpSteps, NewSQLMigrationStep(upSQL))
	if f.PostHook != nil {
		postStep := NewHookMigrationStep().WithUpHook(
			func(ctx context.Context, exec Executor) error {
				return f.PostHook(ctx, exec, f.FilePath)
			},
		)
		mig.UpSteps = append(mig.UpSteps, postStep)
	}
	if f.PreHook != nil {
		preStep := NewHookMigrationStep().WithDownHook(
			func(ctx context.Context, exec Executor) error {
				return f.PreHook(ctx, exec, f.FilePath)
			},
		)
		mig.DownSteps = append(mig.DownSteps, preStep)
	}
	mig.DownSteps = append(mig.DownSteps, NewSQLMigrationStep(downSQL))
	if f.PostHook != nil {
		postStep := NewHookMigrationStep().WithDownHook(
			func(ctx context.Context, exec Executor) error {
				return f.PostHook(ctx, exec, f.FilePath)
			},
		)
		mig.DownSteps = append(mig.DownSteps, postStep)
	}
	log.Printf("Loaded migration from file: %s", f.FilePath)
	return []Migration{*mig}, nil
}

// VarMigrationSource uses SQL queries defined in variables.
type VarMigrationSource struct {
	Version string
	Name    string
	UpSQL   string
	DownSQL string
}

// NewVarMigrationSource creates a new VarMigrationSource.
//
// Parameters:
//   - version: The version of the migration.
//   - name: The name of the migration.
//   - upSQL: The SQL to execute when applying the migration.
//   - downSQL: The SQL to execute when removing the migration.
//
// Returns:
//   - *VarMigrationSource: A new VarMigrationSource.
func NewVarMigrationSource(
	version string, name string, upSQL string, downSQL string,
) *VarMigrationSource {
	return &VarMigrationSource{
		Version: version,
		Name:    name,
		UpSQL:   upSQL,
		DownSQL: downSQL,
	}
}

// LoadMigrations loads the variable-defined migration.
//
// Returns:
//   - []Migration: A slice containing the loaded migration.
//   - error: An error if loading fails.
func (v *VarMigrationSource) LoadMigrations() ([]Migration, error) {
	mig := NewMigration(v.Version, v.Name).
		WithUpSteps([]MigrationStep{NewSQLMigrationStep(v.UpSQL)}).
		WithDownSteps([]MigrationStep{NewSQLMigrationStep(v.DownSQL)})
	log.Printf("Loaded var migration: version %s, name %s", v.Version, v.Name)
	return []Migration{*mig}, nil
}
