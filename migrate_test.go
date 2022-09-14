package migrate_test

import (
	"bytes"
	"embed"
	"io"
	"io/ioutil"
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

	golden, _ := ioutil.ReadFile(filepath.Join(testdata, "output", "all.golden"))
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

	golden, _ := ioutil.ReadFile(filepath.Join(testdata, "output", "all.golden"))
	if diff := cmp.Diff(string(golden), buffer.String()); diff != "" {
		t.Errorf("migration doesn't match golden (-want +got):\n%s", diff)
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

	golden, _ := ioutil.ReadFile(filepath.Join(testdata, "output", "embed.golden"))
	if diff := cmp.Diff(string(golden), buffer.String()); diff != "" {
		t.Errorf("migration doesn't match golden (-want +got):\n%s", diff)
	}
}
