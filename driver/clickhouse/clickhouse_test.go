package clickhouse_test

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ory/dockertest"
	driver "github.com/shanna/migrate/driver/clickhouse"
)

var config = ""

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("docker: connect %s", err)
	}

	resource, err := pool.Run("clickhouse/clickhouse-server", "latest", []string{
		"CLICKHOUSE_USER=default",
		"CLICKHOUSE_PASSWORD=secret",
		"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
	})
	if err != nil {
		log.Fatalf("docker: start clickhouse %s", err)
	}

	config = fmt.Sprintf("clickhouse://default:secret@localhost:%s/default", resource.GetPort("9000/tcp"))

	err = pool.Retry(func() error {
		db, err := sql.Open("clickhouse", config)
		if err != nil {
			return err
		}
		defer db.Close()
		return db.Ping()
	})
	if err != nil {
		log.Fatalf("could not connect to docker clickhouse: %s", err)
	}

	resource.Expire(60)

	code := m.Run()

	if err := pool.Purge(resource); err != nil {
		log.Printf("docker: purge resource %s", err)
	}

	os.Exit(code)
}

func TestClickHouseMigrateCommit(t *testing.T) {
	migrator, err := driver.New(config)
	if err != nil {
		t.Skipf("clickhouse connect %s", err)
	}

	var migrations = []struct{ name, sql string }{
		{`migrate commit: create table sql`, `CREATE TABLE IF NOT EXISTS default.commit_test (id UInt64, name String) ENGINE = MergeTree() ORDER BY id`},
		{`migrate commit: insert sql`, `INSERT INTO default.commit_test (id, name) VALUES (1, 'woot')`},
		{`migrate commit: multiple sql statements`, `SELECT 1`},
	}

	if err = migrator.Begin(); err != nil {
		t.Fatalf("begin %s", err)
	}

	for _, migration := range migrations {
		if err = migrator.Migrate(migration.name, strings.NewReader(migration.sql)); err != nil {
			t.Fatalf("%s %s", migration.name, err)
		}
	}

	if err = migrator.Commit(); err != nil {
		t.Fatalf("commit %s", err)
	}

	db, err := sql.Open("clickhouse", config)
	if err != nil {
		t.Fatalf("post migrate connect %s", err)
	}
	defer db.Close()

	rows, err := db.Query(`SELECT name FROM default.commit_test LIMIT 1`)
	if err != nil {
		t.Fatalf("post migrate select %s", err)
	}
	rows.Close()
}

func TestClickHouseMigrateRollback(t *testing.T) {
	migrator, err := driver.New(config)
	if err != nil {
		t.Skipf("clickhouse connect %s", err)
	}

	var migrations = []struct{ name, sql string }{
		{`migrate rollback: create table sql`, `CREATE TABLE IF NOT EXISTS default.rollback_test (id UInt64, name String) ENGINE = MergeTree() ORDER BY id`},
		{`migrate rollback: insert sql`, `INSERT INTO default.rollback_test (id, name) VALUES (1, 'woot')`},
	}

	if err = migrator.Begin(); err != nil {
		t.Fatalf("begin %s", err)
	}

	for _, migration := range migrations {
		if err = migrator.Migrate(migration.name, strings.NewReader(migration.sql)); err != nil {
			t.Fatalf("%s %s", migration.name, err)
		}
	}

	// ClickHouse doesn't support rollback - this should return an error
	err = migrator.Rollback()
	if err == nil {
		t.Fatalf("expected rollback to return error for clickhouse")
	}

	// Unlike other drivers, the table WILL exist because ClickHouse can't roll back DDL
	db, err := sql.Open("clickhouse", config)
	if err != nil {
		t.Fatalf("post migrate connect %s", err)
	}
	defer db.Close()

	rows, err := db.Query(`SELECT name FROM default.rollback_test LIMIT 1`)
	if err != nil {
		t.Fatalf("table should exist after rollback attempt (clickhouse can't undo DDL): %s", err)
	}
	rows.Close()
}

func TestClickHouseMigrateChecksumCollision(t *testing.T) {
	migrator, err := driver.New(config)
	if err != nil {
		t.Skipf("clickhouse connect %s", err)
	}

	var migrations = []struct{ name, sql string }{
		{`migrate checksum collision`, `CREATE TABLE IF NOT EXISTS default.checksum_test (id UInt64, name String) ENGINE = MergeTree() ORDER BY id`},
		{`migrate checksum collision`, `INSERT INTO default.checksum_test (id, name) VALUES (1, 'woot')`},
	}

	if err = migrator.Begin(); err != nil {
		t.Fatalf("begin %s", err)
	}

	for i, migration := range migrations {
		err := migrator.Migrate(migration.name, strings.NewReader(migration.sql))
		if i == 1 && err == nil {
			t.Fatalf("expected checksum collision but got none")
		}
	}

	if err = migrator.Commit(); err != nil {
		t.Fatalf("commit %s", err)
	}
}
