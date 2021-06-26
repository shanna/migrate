package migrate_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/url"
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

func NewTestMigrator(config *url.URL) (driver.Migrator, error) {
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
	config, _ := url.Parse("test://")

	migrator, err := migrate.New(config)
	if err != nil {
		t.Fatal(err)
	}

	if err := migrator.Dir(filepath.Join(testdata, "input")); err != nil {
		t.Fatal(err)
	}

	golden, _ := ioutil.ReadFile(filepath.Join(testdata, "output", "golden"))
	if diff := cmp.Diff(string(golden), buffer.String()); diff != "" {
		t.Errorf("migration doesn't match golden (-want +got):\n%s", diff)
	}
}

func TestMigrateFS(t *testing.T) {
	testdata := filepath.Join("_testdata")
	config, _ := url.Parse("test://")

	migrator, err := migrate.New(config)
	if err != nil {
		t.Fatal(err)
	}

	if err := migrator.DirFS(os.DirFS(filepath.Join(testdata, "input")), "."); err != nil {
		t.Fatal(err)
	}

	golden, _ := ioutil.ReadFile(filepath.Join(testdata, "output", "golden"))
	if diff := cmp.Diff(string(golden), buffer.String()); diff != "" {
		t.Errorf("migration doesn't match golden (-want +got):\n%s", diff)
	}
}
