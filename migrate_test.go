package migrate_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/shanna/migrate"
	"github.com/shanna/migrate/driver"
)

// Test driver.

func init() {
	driver.Register("test", &TestDriver{})
}

type TestDriver struct{}
type TestMigrator struct{}

func (t *TestDriver) Begin(config string) (driver.Migrator, error) {
	return &TestMigrator{}, nil
}
func (t *TestMigrator) Rollback() error {
	fmt.Println("Rollback called")
	return nil
}

func (t *TestMigrator) Commit() error {
	fmt.Println("Commit called")
	return nil
}

func (t *TestMigrator) Migrate(name string, data io.Reader) error {
	bytes, err := ioutil.ReadAll(data)
	if err != nil {
		return err
	}
	fmt.Printf("%s: %s", name, bytes)
	return nil
}

// Tests

func TestMigrate(t *testing.T) {
	testdata := filepath.Join("_testdata")

	migrator, err := migrate.New("test", "config")
	if err != nil {
		t.Fatal(err)
	}

	if err := migrator.Dir(testdata); err != nil {
		t.Fatal(err)
	}
}
