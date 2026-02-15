package duckdb

import (
	"context"
	"crypto/sha512"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/shanna/migrate/driver"
)

func init() {
	driver.Register("duckdb", New)
}

type migrate struct {
	name      string
	completed time.Time
	checksum  string
}

type DuckDB struct {
	db        *sql.DB
	tx        *sql.Tx
	logger    driver.Logger
	catalog   string
	schema    string
	tableName string
}

func New(dsn string, opts ...driver.Option) (driver.Migrator, error) {
	config := &driver.Config{
		Logger:    slog.Default(),
		Schema:    driver.DefaultSchema,
		TableName: driver.DefaultTableName,
	}
	for _, opt := range opts {
		opt(config)
	}

	conn, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("sql open: %w", err)
	}

	var catalog string
	if err := conn.QueryRow("SELECT current_catalog()").Scan(&catalog); err != nil {
		conn.Close()
		return nil, fmt.Errorf("get current catalog: %w", err)
	}

	d := &DuckDB{
		db:        conn,
		logger:    config.Logger,
		catalog:   catalog,
		schema:    config.Schema,
		tableName: config.TableName,
	}

	return d, nil
}

func (d *DuckDB) qualifiedSchemaName() string {
	return d.catalog + "." + d.schema
}

func (d *DuckDB) qualifiedTableName() string {
	return d.catalog + "." + d.schema + "." + d.tableName
}

func (d *DuckDB) setupSQL() string {
	return fmt.Sprintf(`
create schema if not exists %s;

create table if not exists %s (
  name text not null,
  checksum text not null,
  completed timestamp not null default current_timestamp,
  unique(name, checksum)
);
`, d.qualifiedSchemaName(), d.qualifiedTableName())
}

func (d *DuckDB) selectMigrationSQL() string {
	return fmt.Sprintf(`
select name, completed, checksum
from %s
where name = ?;
`, d.qualifiedTableName())
}

func (d *DuckDB) insertMigrationSQL() string {
	return fmt.Sprintf(`insert into %s (name, checksum) values (?, ?)`, d.qualifiedTableName())
}

func (d *DuckDB) Begin() error {
	ctx := context.TODO()

	transaction, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	// Setup creates schema/table if needed.
	// DuckDB uses file-level locking for serialization.
	if _, err := transaction.Exec(d.setupSQL()); err != nil {
		transaction.Rollback()
		return fmt.Errorf("setup: %w", err)
	}

	d.tx = transaction
	return nil
}

func (d *DuckDB) Rollback() error {
	defer d.db.Close()
	return d.tx.Rollback()
}

func (d *DuckDB) Commit() error {
	defer d.db.Close()
	return d.tx.Commit()
}

func (d *DuckDB) Migrate(name string, data io.Reader) error {
	// Shame you can't stream statements to the driver as well.
	checksum := sha512.New()
	reader := io.TeeReader(data, checksum)
	statements, err := io.ReadAll(reader)
	if err != nil {
		d.tx.Rollback()
		return fmt.Errorf("read: %w", err)
	}

	rows, err := d.tx.Query(d.selectMigrationSQL(), name)
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

		d.logger.Debug(fmt.Sprintf("migrate skip %s", name), "driver", "duckdb", "completed", previous.completed)
		return nil
	}
	rows.Close()

	if _, err := d.tx.Exec(string(statements)); err != nil {
		d.logger.Error(fmt.Sprintf("migrate error %s", name), "driver", "duckdb", "error", err, "sql", string(statements))
		return err
	}

	if _, err = d.tx.Exec(d.insertMigrationSQL(), name, base64.StdEncoding.EncodeToString(checksum.Sum(nil))); err != nil {
		return fmt.Errorf("schema_migrations insert %s", err)
	}

	d.logger.Debug(fmt.Sprintf("migrate %s", name), "driver", "duckdb")
	return nil
}
