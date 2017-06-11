package migrate_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"path/filepath"
	"testing"

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
	bytes, err := ioutil.ReadAll(data)
	if err != nil {
		return err
	}
	buffer.WriteString(fmt.Sprintf("%s:%s", name, bytes))
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
	if !bytes.Equal(buffer.Bytes(), golden) {
		t.Error("migration doesn't match golden")
	}
}
