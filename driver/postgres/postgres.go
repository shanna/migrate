package postgres

import (
	"bytes"
	"context"
	"crypto/sha512"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/shanna/migrate/driver"
)

func init() {
	driver.Register("pg", New)
	driver.Register("postgres", New)
	driver.Register("postgresql", New)
}

type migrate struct {
	name      string
	completed time.Time
	checksum  []byte
}

type Postgres struct {
	db        *pgx.Conn
	tx        pgx.Tx
	schema    string
	tableName string
}

func New(dsn string, opts ...driver.Option) (driver.Migrator, error) {
	config := &driver.Config{
		Schema:    driver.DefaultSchema,
		TableName: driver.DefaultTableName,
	}
	for _, opt := range opts {
		opt(config)
	}

	connConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	connection, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, err
	}

	if err := connection.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping failed %s", err)
	}

	pg := &Postgres{
		db:        connection,
		schema:    config.Schema,
		tableName: config.TableName,
	}

	return pg, nil
}

func (p *Postgres) qualifiedTableName() string {
	return p.schema + "." + p.tableName
}

func (p *Postgres) setupSQL() string {
	return fmt.Sprintf(`
create schema if not exists %s;

create table if not exists %s (
  name text not null primary key,
  checksum bytea not null,
  completed timestamp with time zone not null default now(),
  unique(name, checksum)
);

lock table %s in exclusive mode;
`, p.schema, p.qualifiedTableName(), p.qualifiedTableName())
}

func (p *Postgres) selectMigrationSQL() string {
	return fmt.Sprintf(`
select name, completed, checksum
from %s
where name = $1::text;
`, p.qualifiedTableName())
}

func (p *Postgres) insertMigrationSQL() string {
	return fmt.Sprintf(`
insert into %s (name, checksum) values ($1::text, $2::bytea)
`, p.qualifiedTableName())
}

func (p *Postgres) Begin() error {
	ctx := context.Background()

	if err := p.db.Ping(ctx); err != nil {
		return fmt.Errorf("ping failed %s", err)
	}

	transaction, err := p.db.Begin(ctx)
	if err != nil {
		return err
	}

	// Setup creates schema/table if needed and locks the table.
	// The lock serializes concurrent migrations.
	if _, err := transaction.Exec(ctx, p.setupSQL()); err != nil {
		transaction.Rollback(ctx)
		return fmt.Errorf("setup: %w", err)
	}

	p.tx = transaction
	return nil
}

func (p *Postgres) Rollback() error {
	ctx := context.Background()

	defer p.db.Close(ctx)
	return p.tx.Rollback(ctx)
}

func (p *Postgres) Commit() error {
	ctx := context.Background()

	defer p.db.Close(ctx)
	return p.tx.Commit(ctx)
}

func (p *Postgres) Migrate(name string, data io.Reader) error {
	ctx := context.Background()
	log := slog.With("name", filepath.Base(name), "path", filepath.Dir(name), "driver", "postgres")

	if err := p.db.Ping(ctx); err != nil {
		return fmt.Errorf("ping failed %s", err)
	}

	// Shame you can't stream statements to the driver as well.
	checksum := sha512.New()
	reader := io.TeeReader(data, checksum)
	statements, err := io.ReadAll(reader)
	if err != nil {
		p.tx.Rollback(ctx)
		return fmt.Errorf("read %s", err)
	}

	rows, err := p.tx.Query(ctx, p.selectMigrationSQL(), name)
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
		if !bytes.Equal(checksum.Sum(nil), previous.checksum) {
			return fmt.Errorf("%q has been altered since it was run on %s", previous.name, previous.completed)
		}

		log.Debug("skip", "reason", "already run", "completed", previous.completed)
		return nil
	}
	rows.Close()

	if _, err := p.tx.Exec(ctx, string(statements)); err != nil {
		var pgErr *pgconn.PgError
		// if perr, ok := err.(*pgconn.PgError); ok {
		if errors.As(err, &pgErr) {
			log.Error("error", "error", err, "code", pgErr.Code, "line", pgErr.Line, "sql", string(statements))
		} else {
			log.Error("error", "error", err, "sql", string(statements))
		}
		return err
	}

	if _, err = p.tx.Exec(ctx, p.insertMigrationSQL(), name, checksum.Sum(nil)); err != nil {
		return fmt.Errorf("schema_migrations insert %s", err)
	}

	log.Debug("commit")
	return nil
}
