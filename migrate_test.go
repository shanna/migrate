package migrate_test

import (
	"bytes"
	"embed"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/shanna/migrate"
	"github.com/shanna/migrate/driver"
)

// Test driver.

func init() {
	driver.Register("test", NewTestMigrator)
	driver.Register("test-names", NewNameCapturingMigrator)
}

var buffer bytes.Buffer

type TestMigrator struct{}

func NewTestMigrator(dsn string) (driver.Migrator, error) {
	return &TestMigrator{}, nil
}

func (t *TestMigrator) Begin() error {
	buffer = *new(bytes.Buffer)
	buffer.WriteString("begin\n")
	return nil
}

func (t *TestMigrator) Rollback() error {
	buffer.WriteString("rollback\n")
	return nil
}

func (t *TestMigrator) Commit() error {
	buffer.WriteString("commit\n")
	return nil
}

func (t *TestMigrator) Migrate(name string, data io.Reader) error {
	bytes, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	buffer.Write(bytes)
	return nil
}

// NameCapturingMigrator captures migration names for testing NameFunc.
var capturedNames []string

type NameCapturingMigrator struct{}

func NewNameCapturingMigrator(dsn string) (driver.Migrator, error) {
	return &NameCapturingMigrator{}, nil
}

func (n *NameCapturingMigrator) Begin() error {
	capturedNames = nil
	return nil
}

func (n *NameCapturingMigrator) Rollback() error { return nil }
func (n *NameCapturingMigrator) Commit() error   { return nil }

func (n *NameCapturingMigrator) Migrate(name string, data io.Reader) error {
	capturedNames = append(capturedNames, name)
	io.Copy(io.Discard, data)
	return nil
}

// Tests

func TestMigrate(t *testing.T) {
	testdata := filepath.Join("_testdata")

	migrator, err := migrate.New("test", "test://")
	if err != nil {
		t.Fatal(err)
	}

	if err := migrator.Dir(filepath.Join(testdata, "input")); err != nil {
		t.Fatal(err)
	}

	golden, _ := os.ReadFile(filepath.Join(testdata, "output", "all.golden"))
	if diff := cmp.Diff(string(golden), buffer.String()); diff != "" {
		t.Errorf("migration doesn't match golden (-want +got):\n%s", diff)
	}
}

func TestMigrateFS(t *testing.T) {
	testdata := filepath.Join("_testdata")

	migrator, err := migrate.New("test", "test://")
	if err != nil {
		t.Fatal(err)
	}

	if err := migrator.DirFS(os.DirFS(filepath.Join(testdata, "input")), "."); err != nil {
		t.Fatal(err)
	}

	golden, _ := os.ReadFile(filepath.Join(testdata, "output", "all.golden"))
	if diff := cmp.Diff(string(golden), buffer.String()); diff != "" {
		t.Errorf("migration doesn't match golden (-want +got):\n%s", diff)
	}
}

func TestNameFuncDefault(t *testing.T) {
	migrator, err := migrate.New("test-names", "test://")
	if err != nil {
		t.Fatal(err)
	}

	if err := migrator.Dir(filepath.Join("_testdata", "input")); err != nil {
		t.Fatal(err)
	}

	// Default NameFunc is filepath.Base, so we should get just filenames
	want := []string{"001-test.sql", "002-test.sql", "003-test.sh"}
	if diff := cmp.Diff(want, capturedNames); diff != "" {
		t.Errorf("names mismatch (-want +got):\n%s", diff)
	}
}

func TestNameFuncCustom(t *testing.T) {
	var pathsReceived []string
	customNameFunc := func(path string) string {
		pathsReceived = append(pathsReceived, path)
		return "custom:" + path
	}

	migrator, err := migrate.New("test-names", "test://", migrate.WithNameFunc(customNameFunc))
	if err != nil {
		t.Fatal(err)
	}

	if err := migrator.Dir(filepath.Join("_testdata", "input")); err != nil {
		t.Fatal(err)
	}

	// NameFunc should receive full paths
	wantPaths := []string{
		filepath.Join("_testdata", "input", "001-test.sql"),
		filepath.Join("_testdata", "input", "002-test.sql"),
		filepath.Join("_testdata", "input", "003-test.sh"),
	}
	if diff := cmp.Diff(wantPaths, pathsReceived); diff != "" {
		t.Errorf("paths received by NameFunc mismatch (-want +got):\n%s", diff)
	}

	// Migration names should be transformed
	wantNames := []string{
		"custom:" + filepath.Join("_testdata", "input", "001-test.sql"),
		"custom:" + filepath.Join("_testdata", "input", "002-test.sql"),
		"custom:" + filepath.Join("_testdata", "input", "003-test.sh"),
	}
	if diff := cmp.Diff(wantNames, capturedNames); diff != "" {
		t.Errorf("migration names mismatch (-want +got):\n%s", diff)
	}
}

//go:embed _testdata/input/*.sql
var input embed.FS

func TestMigrateEmbedFS(t *testing.T) {
	testdata := filepath.Join("_testdata")

	migrator, err := migrate.New("test", "test://")
	if err != nil {
		t.Fatal(err)
	}

	if err := migrator.DirFS(input, filepath.Join(testdata, "input")); err != nil {
		t.Fatal(err)
	}

	golden, _ := os.ReadFile(filepath.Join(testdata, "output", "embed.golden"))
	if diff := cmp.Diff(string(golden), buffer.String()); diff != "" {
		t.Errorf("migration doesn't match golden (-want +got):\n%s", diff)
	}
}
