package postgres_test

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ory/dockertest"
	driver "github.com/shanna/migrate/driver/postgres"
)

var config = ""

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("docker: connect %s", err)
	}

	resource, err := pool.Run("postgres", "15-alpine", []string{"POSTGRES_PASSWORD=secret", "POSTGRES_DB=migrate"})
	if err != nil {
		log.Fatalf("docker: start postgres %s", err)
	}

	config = fmt.Sprintf("postgres://postgres:secret@localhost:%s/migrate?sslmode=disable", resource.GetPort("5432/tcp"))

	err = pool.Retry(func() error {
		db, err := sql.Open("pgx", config)
		if err != nil {
			return err
		}
		return db.Ping()
	})
	if err != nil {
		log.Fatalf("could not connect to docker postgres: %s", err)
	}

	// Kill the container if tests take longer than 60 seconds.
	resource.Expire(60)

	code := m.Run()

	if err := pool.Purge(resource); err != nil {
		log.Printf("docker: purge resource %s", err)
	}

	os.Exit(code)
}

func TestPostgresMigrateCommit(t *testing.T) {
	migrator, err := driver.New(config)
	if err != nil {
		t.Skipf("postgres connect %s", err)
	}

	var migrations = []struct{ name, sql string }{
		{`migrate commit: create table sql`, `create table commit_test (id serial, name text)`},
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

	db, err := sql.Open("pgx", config)
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

func TestPostgresMigrateRollback(t *testing.T) {
	migrator, err := driver.New(config)
	if err != nil {
		t.Skipf("postgres connect %s", err)
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

	db, err := sql.Open("pgx", config)
	if err != nil {
		t.Fatalf("post migrate connnect %s", err)
	}
	defer db.Close()

	// Table should not exist after rollback
	_, err = db.Query(`select name from rollback_test limit 1`)
	if err == nil {
		t.Fatalf("expected table to not exist after rollback")
	}
}
