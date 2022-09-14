package postgres_test

import (
	"database/sql"
	"os"
	"strings"
	"testing"

	driver "github.com/shanna/migrate/driver/sqlite"
	_ "modernc.org/sqlite"
)

func TestSqliteMigrateCommit(t *testing.T) {
	dir, err := os.MkdirTemp("", "migrate-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	migrator, err := driver.New("file:" + dir + "/migrate.db")
	if err != nil {
		t.Skipf("connect %s", err)
	}

	var migrations = []struct{ name, sql string }{
		{`migrate commit: create table sql`, `create table commit_test (id text, name text)`},
		{`migrate commit: insert sql`, `insert into commit_test (name) values ('woot')`},
		{`migrate commit: multiple sql statements`, `select 1; select * from commit_test limit 1`},
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

	db, err := sql.Open("sqlite", "file:"+dir+"/migrate.db")
	if err != nil {
		t.Fatalf("post migrate connnect %s", err)
	}
	defer db.Close()

	rows, err := db.Query(`select name from commit_test limit 1;`)
	if err != nil {
		t.Fatalf("post migrate select %s", err)
	}
	rows.Close()
}

func TestSqliteMigrateRollback(t *testing.T) {
	dir, err := os.MkdirTemp("", "migrate-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	migrator, err := driver.New("file:" + dir + "/migrate.db")
	if err != nil {
		t.Skipf("connect %s", err)
	}

	var migrations = []struct{ name, sql string }{
		{`migrate rollback: create table sql`, `create table rollback_test (id serial, name text)`},
		{`migrate rollback: insert sql`, `insert into rollback_test (name) values ('woot')`},
		{`migrate rollback: multiple sql statements`, `select 1; select * from rollback_test limit 1`},
	}

	if err = migrator.Begin(); err != nil {
		t.Fatalf("begin %s", err)
	}

	for _, migration := range migrations {
		if err = migrator.Migrate(migration.name, strings.NewReader(migration.sql)); err != nil {
			t.Fatalf("%s %s", migration.name, err)
		}
	}

	if err = migrator.Rollback(); err != nil {
		t.Fatalf("rollback %s", err)
	}

	db, err := sql.Open("sqlite", "file:"+dir+"/migrate.db")
	if err != nil {
		t.Fatalf("post migrate connnect %s", err)
	}
	defer db.Close()

	rows, err := db.Query(`select name from rollback_test limit 1`)
	if err != nil {
		t.Fatalf("error %s", err)
	}
	rows.Close()
}

func TestSqliteMigrateChecksumRollback(t *testing.T) {
	dir, err := os.MkdirTemp("", "migrate-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	migrator, err := driver.New("file:" + dir + "/migrate.db")
	if err != nil {
		t.Skipf("connect %s", err)
	}

	var migrations = []struct{ name, sql string }{
		{`migrate checksum collision`, `create table rollback_test (id serial, name text)`},
		{`migrate checksum collision`, `insert into rollback_test (name) values ('woot')`},
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

	if err = migrator.Rollback(); err != nil {
		t.Fatalf("rollback %s", err)
	}

	db, err := sql.Open("sqlite", "file:"+dir+"/migrate.db")
	if err != nil {
		t.Fatalf("post migrate connnect %s", err)
	}
	defer db.Close()

	rows, err := db.Query(`select name from rollback_test limit 1`)
	if err != nil {
		t.Fatalf("error %s", err)
	}
	if rows.Next() {
		t.Fatalf("expected no rows")
	}
	rows.Close()
}
