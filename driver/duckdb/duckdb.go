package duckdb

import (
	"context"
	"crypto/sha512"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/shanna/migrate/driver"
)

func init() {
	driver.Register("duckdb", New)
}

const SetupSQL = `
create table if not exists schema_migrations (
  name text not null,
  checksum text not null,
  completed timestamp not null default current_timestamp,
  unique(name, checksum)
);
`

const MigrateSQL = `
select name, completed, checksum
from schema_migrations
where name = ?;
`

type migrate struct {
	name      string
	completed time.Time
	checksum  string
}

type DuckDB struct {
	db *sql.DB
	tx *sql.Tx
}

func New(dsn string) (driver.Migrator, error) {
	conn, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("sql open: %w", err)
	}

	d := &DuckDB{
		db: conn,
	}

	// Migration table.
	transaction, err := conn.Begin()
	if err != nil {
		return nil, err
	}
	defer transaction.Rollback()
	if _, err = transaction.Exec(SetupSQL); err != nil {
		return nil, err
	}
	transaction.Commit()

	return d, nil
}

func (d *DuckDB) Begin() error {
	ctx := context.TODO()

	transaction, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	d.tx = transaction
	return nil
}

func (d *DuckDB) Rollback() error {
	defer d.db.Close()
	return d.tx.Commit()
}

func (d *DuckDB) Commit() error {
	defer d.db.Close()
	return d.tx.Commit()
}

func (d *DuckDB) Migrate(name string, data io.Reader) error {
	log := slog.With("name", filepath.Base(name), "path", filepath.Dir(name), "driver", "duckdb")

	// Shame you can't stream statements to the driver as well.
	checksum := sha512.New()
	reader := io.TeeReader(data, checksum)
	statements, err := io.ReadAll(reader)
	if err != nil {
		d.tx.Rollback()
		return fmt.Errorf("read: %w", err)
	}

	rows, err := d.tx.Query(MigrateSQL, name)
	if err != nil {
		return fmt.Errorf("schema_migrations select previous %s", err)
	}
	defer rows.Close()

	if rows.Next() {
		previous := migrate{}
		err = rows.Scan(&previous.name, &previous.completed, &previous.checksum)
		if err != nil {
			return fmt.Errorf("schema_migrations scan previous %s", err)
		}
		if base64.StdEncoding.EncodeToString(checksum.Sum(nil)) != previous.checksum {
			return fmt.Errorf("%q has been altered since it was run on %s", previous.name, previous.completed)
		}

		log.Debug("skip", "reason", "already run", "completed", previous.completed)
		return nil
	}
	rows.Close()

	if _, err := d.tx.Exec(string(statements)); err != nil {
		log.Error("error", "error", err, "sql", string(statements))
		return err
	}

	if _, err = d.tx.Exec(`insert into schema_migrations (name, checksum) values (?, ?)`, name, base64.StdEncoding.EncodeToString(checksum.Sum(nil))); err != nil {
		return fmt.Errorf("schema_migrations insert %s", err)
	}

	log.Debug("commit")
	return nil
}
