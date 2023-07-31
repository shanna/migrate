package postgres

import (
	"bytes"
	"context"
	"crypto/sha512"
	"errors"
	"fmt"
	"io"
	"log"
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

const SetupSQL = `
create schema if not exists migrate;

create table if not exists migrate.schema_migrations (
  name text not null primary key,
  checksum bytea not null,
  completed timestamp with time zone not null default now(),
  unique(name, checksum)
);
lock table migrate.schema_migrations in exclusive mode;
`

const SelectMigrationSQL = `
select name, completed, checksum
from migrate.schema_migrations
where name = $1::text;
`

const InsertMigrationSQL = `
insert into migrate.schema_migrations (name, checksum) values ($1::text, $2::bytea)
`

type migrate struct {
	name      string
	completed time.Time
	checksum  []byte
}

type Postgres struct {
	db *pgx.Conn
	tx pgx.Tx
}

func New(dsn string) (driver.Migrator, error) {
	connConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	pg := &Postgres{}

	connection, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, err
	}

	if err := connection.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping failed %s", err)
	}

	// Migration table.
	transaction, err := connection.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer transaction.Rollback(ctx)
	if _, err = transaction.Exec(ctx, SetupSQL); err != nil {
		return nil, err
	}
	transaction.Commit(ctx)

	pg.db = connection

	return pg, nil
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

	p.tx = transaction
	return nil
}

func (p *Postgres) Rollback() error {
	ctx := context.Background()

	defer p.db.Close(ctx)
	return p.tx.Commit(ctx)
}

func (p *Postgres) Commit() error {
	ctx := context.Background()

	defer p.db.Close(ctx)
	return p.tx.Commit(ctx)
}

func (p *Postgres) Migrate(name string, data io.Reader) error {
	ctx := context.Background()

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

	rows, err := p.tx.Query(ctx, SelectMigrationSQL, name)
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

		log.Printf("%s: skip, already run on %s", previous.name, previous.completed)
		return nil
	}
	rows.Close()

	if _, err := p.tx.Exec(ctx, string(statements)); err != nil {
		var pgErr *pgconn.PgError
		// if perr, ok := err.(*pgconn.PgError); ok {
		if errors.As(err, &pgErr) {
			log.Printf("%s: error:%s, code:%s, line:%d, sql: <<SQL\n%s\nSQL", name, err, pgErr.Code, pgErr.Line, string(statements))
		} else {
			log.Printf("%s: error: %s, sql: <<SQL\n%s\nSQL", name, err, string(statements))
		}
		return err
	}

	if _, err = p.tx.Exec(ctx, InsertMigrationSQL, name, checksum.Sum(nil)); err != nil {
		return fmt.Errorf("schema_migrations insert %s", err)
	}

	log.Printf("%s: commit", name) // TODO: Log in the actual commit with per migration log context.
	return nil
}
