package migrator

import (
    "context"
    "database/sql"
    "database/sql/driver"
    "errors"
    "io"
    "os"
    "path/filepath"
    "strings"
    "sync"
    "testing"
)

// --- Test Driver & Fakes ---

type record struct{
    query string
}

type testDrv struct{}
type testConn struct{}
type testTx struct{}
type testResult struct{}
type testRows struct{
    cols []string
    data [][]driver.Value
    i int
}

var (
    recMu sync.Mutex
    recs  []record
    txCommits int
    txRollbacks int
    rowsMu sync.Mutex
    rowsForNextQuery [][]driver.Value
)

func addRec(q string){
    recMu.Lock(); defer recMu.Unlock()
    recs = append(recs, record{query: q})
}

func resetRecs(){
    recMu.Lock(); defer recMu.Unlock()
    recs = nil
}

func (d testDrv) Open(name string) (driver.Conn, error) { return testConn{}, nil }
func (c testConn) Prepare(query string) (driver.Stmt, error) { return nil, errors.New("not implemented") }
func (c testConn) Close() error { return nil }
func (c testConn) Begin() (driver.Tx, error) { return testTx{}, nil }
func (c testConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) { return testTx{}, nil }
func (t testTx) Commit() error { recMu.Lock(); txCommits++; recMu.Unlock(); return nil }
func (t testTx) Rollback() error { recMu.Lock(); txRollbacks++; recMu.Unlock(); return nil }

// ExecContext support
func (c testConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
    addRec(query)
    if query == "FAIL" { return nil, errors.New("forced exec failure") }
    return testResult{}, nil
}
func (c testConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
    addRec(query)
    if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "SELECT") {
        rowsMu.Lock()
        data := rowsForNextQuery
        rowsForNextQuery = nil
        rowsMu.Unlock()
        if data == nil { data = [][]driver.Value{} }
        return &testRows{cols: []string{"version"}, data: data}, nil
    }
    return nil, errors.New("not implemented")
}
func (c testConn) CheckNamedValue(*driver.NamedValue) error { return nil }
func (testResult) LastInsertId() (int64, error) { return 0, nil }
func (testResult) RowsAffected() (int64, error) { return 1, nil }
func (r *testRows) Columns() []string { return r.cols }
func (r *testRows) Close() error { return nil }
func (r *testRows) Next(dest []driver.Value) error {
    if r.i >= len(r.data) { return io.EOF }
    copy(dest, r.data[r.i])
    r.i++
    return nil
}

// mark interfaces implemented
var _ driver.Driver = testDrv{}
var _ driver.Conn = testConn{}
var _ driver.ExecerContext = testConn{}
var _ driver.QueryerContext = testConn{}
var _ driver.ConnBeginTx = testConn{}
var _ driver.NamedValueChecker = testConn{}

func init(){
    sql.Register("testdrv", testDrv{})
}

// Fake history manager capturing calls without hitting DB
type fakeHistory struct{
    ensured bool
    recorded []Migration
    removed  []Migration
    applied  map[string]bool
}

func (f *fakeHistory) EnsureHistoryTable(ctx context.Context, db *sql.DB, table string) error {
    f.ensured = true
    return nil
}
func (f *fakeHistory) RecordMigration(ctx context.Context, exec Executor, table string, mig Migration, name string) error {
    f.recorded = append(f.recorded, mig)
    return nil
}
func (f *fakeHistory) RemoveMigration(ctx context.Context, exec Executor, table string, mig Migration, name string) error {
    f.removed = append(f.removed, mig)
    return nil
}
func (f *fakeHistory) AppliedMigrations(ctx context.Context, db *sql.DB, table string, name string) (map[string]bool, error) {
    if f.applied == nil { return map[string]bool{}, nil }
    return f.applied, nil
}

// --- Tests ---

func TestDefaultParseFilename(t *testing.T){
    v, n, d, ok := defaultParseFilename("001_init_up.sql")
    if !ok || v != "001" || n != "init" || d != "up" {
        t.Fatalf("unexpected parse: %v %v %v ok=%v", v, n, d, ok)
    }
}

func TestDirMigrationSource_LoadMigrations_ParsesSortsAndHooks(t *testing.T){
    dir := t.TempDir()
    // create files
    mustWrite(t, filepath.Join(dir, "001_init_up.sql"), "CREATE TABLE t1(x int);")
    mustWrite(t, filepath.Join(dir, "001_init_down.sql"), "DROP TABLE t1;")
    mustWrite(t, filepath.Join(dir, "010_users_up.sql"), "CREATE TABLE users(id int);")
    mustWrite(t, filepath.Join(dir, "010_users_down.sql"), "DROP TABLE users;")

    src := NewDirMigrationSource(dir)
    // add hooks only for 001 files
    src.ResolveHooks = func(filename string)(FileHookFn, FileHookFn){
        if filename == "001_init_up.sql" || filename == "001_init_down.sql" {
            return func(ctx context.Context, exec Executor, p string) error { addRec("pre:"+p); return nil },
                func(ctx context.Context, exec Executor, p string) error { addRec("post:"+p); return nil }
        }
        return nil, nil
    }

    migs, err := src.LoadMigrations()
    if err != nil { t.Fatalf("LoadMigrations error: %v", err) }
    if len(migs) != 2 { t.Fatalf("expected 2 versions, got %d", len(migs)) }
    if migs[0].Version != "001" || migs[1].Version != "010" {
        t.Fatalf("expected sorted versions [001,010], got [%s,%s]", migs[0].Version, migs[1].Version)
    }

    // verify hook placement on first migration up/down
    m := migs[0]
    if len(m.UpSteps) != 3 || len(m.DownSteps) != 3 {
        t.Fatalf("expected 3 steps up/down with hooks, got up=%d down=%d", len(m.UpSteps), len(m.DownSteps))
    }
}

func TestFileMigrationSource_LoadMigrations_SplitAndHooks(t *testing.T){
    f := filepath.Join(t.TempDir(), "001_single.sql")
    mustWrite(t, f, "CREATE A;\n-- DOWN\nDROP A;")
    src := NewFileMigrationSource(f)
    src.PreHook = func(ctx context.Context, exec Executor, p string) error { addRec("pre:"+p); return nil }
    src.PostHook = func(ctx context.Context, exec Executor, p string) error { addRec("post:"+p); return nil }

    migs, err := src.LoadMigrations()
    if err != nil { t.Fatalf("LoadMigrations error: %v", err) }
    if len(migs) != 1 { t.Fatalf("expected 1 migration, got %d", len(migs)) }
    m := migs[0]
    if len(m.UpSteps) != 3 || len(m.DownSteps) != 3 {
        t.Fatalf("expected 3 steps up/down with hooks, got up=%d down=%d", len(m.UpSteps), len(m.DownSteps))
    }
}

func TestMigrator_LoadAllMigrations_SortsAndValidates(t *testing.T){
    // valid: sorts ascending
    s1 := &staticSource{migs: []Migration{
        *NewMigration("002", "b").WithUpSteps([]MigrationStep{NewSQLMigrationStep("B")}),
        *NewMigration("001", "a").WithUpSteps([]MigrationStep{NewSQLMigrationStep("A")}),
    }}
    m := &Migrator{}
    m = m.WithSources([]MigrationSource{s1})
    got, err := m.LoadAllMigrations()
    if err != nil { t.Fatalf("LoadAllMigrations error: %v", err) }
    if got[0].Version != "001" || got[1].Version != "002" {
        t.Fatalf("expected sorted versions [001,002], got [%s,%s]", got[0].Version, got[1].Version)
    }

    // invalid: missing up steps
    s2 := &staticSource{migs: []Migration{ *NewMigration("003", "bad") }}
    m = m.WithSources([]MigrationSource{s2})
    if _, err := m.LoadAllMigrations(); err == nil {
        t.Fatalf("expected error for missing up steps")
    }
}

func TestMigrator_MigrateUpAndDown_WithFakeDBAndHistory(t *testing.T){
    resetRecs()
    recMu.Lock(); txCommits, txRollbacks = 0, 0; recMu.Unlock()
    db, err := sql.Open("testdrv", "")
    if err != nil { t.Fatalf("open test driver: %v", err) }
    defer db.Close()

    // one migration with distinct SQL for up/down
    mig := *NewMigration("001", "init")
    mig.UpSteps = []MigrationStep{ NewSQLMigrationStep("UP_SQL") }
    mig.DownSteps = []MigrationStep{ NewSQLMigrationStep("DOWN_SQL") }

    src := &staticSource{migs: []Migration{mig}}
    fh := &fakeHistory{applied: map[string]bool{}}

    m := NewMigrator(db, "schema_migrations", fh, "app").
        WithSources([]MigrationSource{src}).
        WithTransactional(true)

    // Migrate up
    if err := m.MigrateUp(context.Background(), ""); err != nil {
        t.Fatalf("MigrateUp error: %v", err)
    }
    if len(fh.recorded) != 1 || fh.recorded[0].Version != "001" { t.Fatalf("expected record of 001, got %+v", fh.recorded) }
    // Verify our driver saw the up SQL
    if !containsExec("UP_SQL") { t.Fatalf("expected UP_SQL to be executed; recs=%v", recStrings()) }

    // Mark as applied so down runs
    fh.applied = map[string]bool{"001": true}
    // Migrate down
    if err := m.MigrateDown(context.Background(), ""); err != nil {
        t.Fatalf("MigrateDown error: %v", err)
    }
    if len(fh.removed) != 1 || fh.removed[0].Version != "001" { t.Fatalf("expected remove of 001, got %+v", fh.removed) }
    if !containsExec("DOWN_SQL") { t.Fatalf("expected DOWN_SQL to be executed; recs=%v", recStrings()) }
}

func TestVarMigrationSource_LoadMigrations(t *testing.T){
    v := NewVarMigrationSource("005", "vsrc", "UPV", "DOWNV")
    migs, err := v.LoadMigrations()
    if err != nil { t.Fatalf("LoadMigrations error: %v", err) }
    if len(migs) != 1 { t.Fatalf("expected 1 migration, got %d", len(migs)) }
    // execute steps to ensure SQL is wired
    db, _ := sql.Open("testdrv", "")
    defer db.Close()
    if err := migs[0].UpSteps[0].ExecuteUp(context.Background(), db); err != nil { t.Fatalf("exec up: %v", err) }
    if err := migs[0].DownSteps[0].ExecuteDown(context.Background(), db); err != nil { t.Fatalf("exec down: %v", err) }
    if !containsExec("UPV") || !containsExec("DOWNV") { t.Fatalf("expected UPV and DOWNV executed: %v", recStrings()) }
}

func TestMigrator_TargetVersionStopsUpAndDown(t *testing.T){
    resetRecs()
    db, _ := sql.Open("testdrv", "")
    defer db.Close()
    // three migrations
    m1 := *NewMigration("001", "a"); m1.UpSteps = []MigrationStep{NewSQLMigrationStep("UP_001")}; m1.DownSteps = []MigrationStep{NewSQLMigrationStep("DOWN_001")}
    m2 := *NewMigration("002", "b"); m2.UpSteps = []MigrationStep{NewSQLMigrationStep("UP_002")}; m2.DownSteps = []MigrationStep{NewSQLMigrationStep("DOWN_002")}
    m3 := *NewMigration("003", "c"); m3.UpSteps = []MigrationStep{NewSQLMigrationStep("UP_003")}; m3.DownSteps = []MigrationStep{NewSQLMigrationStep("DOWN_003")}
    src := &staticSource{migs: []Migration{m1, m2, m3}}
    fh := &fakeHistory{applied: map[string]bool{}}
    m := NewMigrator(db, "schema_migrations", fh, "app").WithSources([]MigrationSource{src})
    if err := m.MigrateUp(context.Background(), "002"); err != nil { t.Fatalf("MigrateUp: %v", err) }
    if containsExec("UP_003") { t.Fatalf("did not expect UP_003 to run: %v", recStrings()) }
    if !containsExec("UP_001") || !containsExec("UP_002") { t.Fatalf("expected UP_001 and UP_002 executed: %v", recStrings()) }

    // now mark all applied and roll back down to 002 (should only run 003)
    resetRecs()
    fh.applied = map[string]bool{"001":true, "002":true, "003":true}
    if err := m.MigrateDown(context.Background(), "002"); err != nil { t.Fatalf("MigrateDown: %v", err) }
    // Down to target 002 should roll back 003 and 002, leaving 001 applied
    if !containsExec("DOWN_003") || !containsExec("DOWN_002") { t.Fatalf("expected DOWN_003 and DOWN_002 executed: %v", recStrings()) }
    if containsExec("DOWN_001") { t.Fatalf("did not expect DOWN_001: %v", recStrings()) }
}

func TestSQLiteHistoryManager_SQLAndAppliedExtraction(t *testing.T){
    resetRecs()
    db, _ := sql.Open("testdrv", "")
    defer db.Close()
    hm := NewSQLiteHistoryManager()
    ctx := context.Background()
    if err := hm.EnsureHistoryTable(ctx, db, "hist"); err != nil { t.Fatalf("ensure: %v", err) }
    _ = hm.RecordMigration(ctx, db, "hist", *NewMigration("001","a"), "app")
    _ = hm.RemoveMigration(ctx, db, "hist", *NewMigration("001","a"), "app")
    // set rows to be returned for applied
    rowsMu.Lock(); rowsForNextQuery = [][]driver.Value{{"001"},{"002"}}; rowsMu.Unlock()
    applied, err := hm.AppliedMigrations(ctx, db, "hist", "app")
    if err != nil { t.Fatalf("applied: %v", err) }
    if !applied["001"] || !applied["002"] { t.Fatalf("expected versions in applied: %+v", applied) }
    // smoke check recorded SQL text presence
    if !containsSubstr("CREATE TABLE IF NOT EXISTS hist") { t.Fatalf("expected ensure create statement: %v", recStrings()) }
}

func TestTransactionalRollbackOnError(t *testing.T){
    resetRecs(); recMu.Lock(); txCommits, txRollbacks = 0, 0; recMu.Unlock()
    db, _ := sql.Open("testdrv", "")
    defer db.Close()
    // migration will fail on Exec
    mig := *NewMigration("001","bad")
    mig.UpSteps = []MigrationStep{ NewSQLMigrationStep("FAIL") }
    src := &staticSource{migs: []Migration{mig}}
    fh := &fakeHistory{}
    m := NewMigrator(db, "hist", fh, "app").WithSources([]MigrationSource{src}).WithTransactional(true)
    err := m.MigrateUp(context.Background(), "")
    if err == nil { t.Fatalf("expected error from failing step") }
    recMu.Lock(); c, r := txCommits, txRollbacks; recMu.Unlock()
    if c != 0 || r != 1 { t.Fatalf("expected 0 commits and 1 rollback, got c=%d r=%d", c, r) }
    if len(fh.recorded) != 0 { t.Fatalf("expected no recorded migrations on failure") }
}

func TestDirMigrationSource_AllowedExtsAndDefault(t *testing.T){
    resetRecs()
    dir := t.TempDir()
    mustWrite(t, filepath.Join(dir, "002_accept_up.sqlite"), "CREATE TABLE ok(x);")
    mustWrite(t, filepath.Join(dir, "002_accept_down.sqlite"), "DROP TABLE ok;")
    mustWrite(t, filepath.Join(dir, "003_skip.txt"), "ignored")
    // default allows .sqlite
    src := NewDirMigrationSource(dir)
    migs, err := src.LoadMigrations()
    if err != nil { t.Fatalf("LoadMigrations: %v", err) }
    if len(migs) != 1 || migs[0].Version != "002" { t.Fatalf("expected 1 sqlite migration version 002, got %+v", migs) }
    // restrict to .sql only
    src = src.WithAllowedExts([]string{".sql"})
    migs, err = src.LoadMigrations()
    if err != nil { t.Fatalf("LoadMigrations 2: %v", err) }
    if len(migs) != 0 { t.Fatalf("expected 0 migrations when only .sql allowed, got %d", len(migs)) }
}

func TestFileMigrationSource_NoDownSection(t *testing.T){
    resetRecs()
    f := filepath.Join(t.TempDir(), "004_nodown.sql")
    mustWrite(t, f, "CREATE Z;")
    src := NewFileMigrationSource(f)
    migs, err := src.LoadMigrations()
    if err != nil { t.Fatalf("LoadMigrations: %v", err) }
    if len(migs) != 1 { t.Fatalf("expected 1 migration, got %d", len(migs)) }
    // Down step exists but empty
    db, _ := sql.Open("testdrv", ""); defer db.Close()
    if err := migs[0].DownSteps[0].ExecuteDown(context.Background(), db); err != nil { t.Fatalf("down exec: %v", err) }
    // an empty query is recorded
    if !containsExec("") { t.Fatalf("expected empty down query recorded: %v", recStrings()) }
}

func TestHookMigrationStep_MissingHooksReturnError(t *testing.T){
    tStep := NewHookMigrationStep()
    db, _ := sql.Open("testdrv", ""); defer db.Close()
    if err := tStep.ExecuteUp(context.Background(), db); err == nil { t.Fatalf("expected up hook error") }
    if err := tStep.ExecuteDown(context.Background(), db); err == nil { t.Fatalf("expected down hook error") }
}

func TestSQLMigrationStep_WithSQL(t *testing.T){
    step := NewSQLMigrationStep("A").WithSQL("B")
    db, _ := sql.Open("testdrv", ""); defer db.Close()
    if err := step.ExecuteUp(context.Background(), db); err != nil { t.Fatalf("exec up: %v", err) }
    if !containsExec("B") { t.Fatalf("expected 'B' execution: %v", recStrings()) }
}

func TestMySQLHistoryManager_SQLAndAppliedExtraction(t *testing.T){
    resetRecs()
    db, _ := sql.Open("testdrv", ""); defer db.Close()
    hm := NewMySQLHistoryManager()
    ctx := context.Background()
    if err := hm.EnsureHistoryTable(ctx, db, "hist"); err != nil { t.Fatalf("ensure: %v", err) }
    _ = hm.RecordMigration(ctx, db, "hist", *NewMigration("001","a"), "app")
    _ = hm.RemoveMigration(ctx, db, "hist", *NewMigration("001","a"), "app")
    rowsMu.Lock(); rowsForNextQuery = [][]driver.Value{{"001"}}; rowsMu.Unlock()
    applied, err := hm.AppliedMigrations(ctx, db, "hist", "app")
    if err != nil { t.Fatalf("applied: %v", err) }
    if !applied["001"] { t.Fatalf("expected version 001 applied") }
    if !containsSubstr("CREATE TABLE IF NOT EXISTS hist") { t.Fatalf("expected ensure create statement: %v", recStrings()) }
}

func TestMigrator_TransactionalCommitAndNonTransactional(t *testing.T){
    resetRecs(); recMu.Lock(); txCommits, txRollbacks = 0, 0; recMu.Unlock()
    db, _ := sql.Open("testdrv", ""); defer db.Close()
    mig := *NewMigration("001","ok")
    mig.UpSteps = []MigrationStep{ NewSQLMigrationStep("OK") }
    src := &staticSource{migs: []Migration{mig}}
    fh := &fakeHistory{}
    m := NewMigrator(db, "hist", fh, "app").WithSources([]MigrationSource{src}).WithTransactional(true)
    if err := m.MigrateUp(context.Background(), ""); err != nil { t.Fatalf("MigrateUp: %v", err) }
    recMu.Lock(); c, r := txCommits, txRollbacks; recMu.Unlock()
    if c != 1 || r != 0 { t.Fatalf("expected 1 commit, 0 rollbacks; got c=%d r=%d", c, r) }

    // non-transactional: should not change commit/rollback counters
    resetRecs(); recMu.Lock(); txCommits, txRollbacks = 0, 0; recMu.Unlock()
    m2 := NewMigrator(db, "hist", fh, "app").WithSources([]MigrationSource{src}).WithTransactional(false)
    if err := m2.MigrateUp(context.Background(), ""); err != nil { t.Fatalf("MigrateUp non-tx: %v", err) }
    recMu.Lock(); c, r = txCommits, txRollbacks; recMu.Unlock()
    if c != 0 || r != 0 { t.Fatalf("expected 0 commit/rollback in non-tx; got c=%d r=%d", c, r) }
}

func TestMigrator_EnsureHistoryTableCalled(t *testing.T){
    db, _ := sql.Open("testdrv", ""); defer db.Close()
    fh := &fakeHistory{}
    m := NewMigrator(db, "hist", fh, "app")
    if err := m.ensureHistoryTable(context.Background()); err != nil { t.Fatalf("ensureHistoryTable: %v", err) }
    if !fh.ensured { t.Fatalf("expected ensureHistoryTable to call HistoryManager.EnsureHistoryTable") }
}

func TestDirMigrationSource_CustomParser(t *testing.T){
    dir := t.TempDir()
    mustWrite(t, filepath.Join(dir, "weird.ext"), "SELECT 1;")
    src := NewDirMigrationSource(dir).WithAllowedExts([]string{".ext"})
    src = src.WithFilenameParser(func(filename string)(string,string,string,bool){ return "100","custom","up",true })
    migs, err := src.LoadMigrations()
    if err != nil { t.Fatalf("LoadMigrations: %v", err) }
    if len(migs) != 1 || migs[0].Version != "100" || migs[0].Name != "custom" { t.Fatalf("expected custom parsed migration, got %+v", migs) }
}

// --- Helpers ---

type staticSource struct{ migs []Migration }
func (s *staticSource) LoadMigrations() ([]Migration, error) { return s.migs, nil }

func mustWrite(t *testing.T, p, s string){
    t.Helper()
    if err := os.WriteFile(p, []byte(s), 0o600); err != nil { t.Fatalf("write %s: %v", p, err) }
}

func containsExec(sub string) bool {
    recMu.Lock(); defer recMu.Unlock()
    for _, r := range recs { if r.query == sub { return true } }
    return false
}
func containsSubstr(sub string) bool {
    recMu.Lock(); defer recMu.Unlock()
    for _, r := range recs { if strings.Contains(r.query, sub) { return true } }
    return false
}
func recStrings() []string {
    recMu.Lock(); defer recMu.Unlock()
    out := make([]string, len(recs))
    for i, r := range recs { out[i] = r.query }
    return out
}
