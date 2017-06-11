package pq

import (
	"bytes"
	"crypto/sha512"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	_ "github.com/lib/pq"
	"github.com/shanna/migrate/driver"
)

func init() {
	postgres := Driver{}
	driver.Register("pg", &postgres)
	driver.Register("postgres", &postgres)
	driver.Register("postgresql", &postgres)
}

type Driver struct{}

func (d *Driver) Begin(config string) (driver.Migrator, error) {
	return Begin(config)
}

const SetupSQL = `
create table if not exists schema_migrations (
  name text not null,
  checksum bytea not null,
  completed timestamp with time zone not null default now(),
  unique(name, checksum)
);
lock table schema_migrations in exclusive mode;
`

const MigrateSQL = `
select name, completed, checksum
from schema_migrations
where name = $1::text;
`

type migrate struct {
	name      string
	completed time.Time
	checksum  []byte
}

type Postgres struct {
	db *sql.DB
	tx *sql.Tx
	// closed/mutex
}

func Begin(config string) (*Postgres, error) {
	connection, err := sql.Open("postgres", config)
	if err != nil {
		return nil, err
	}

	if err := connection.Ping(); err != nil {
		return nil, fmt.Errorf("ping failed %s", err)
	}

	// Migration table.
	transaction, err := connection.Begin()
	if err != nil {
		return nil, err
	}
	if _, err = transaction.Exec(SetupSQL); err != nil {
		return nil, err
	}

	return &Postgres{connection, transaction}, nil
}

func (p *Postgres) Rollback() error {
	defer p.db.Close()
	return p.tx.Commit()
}

func (p *Postgres) Commit() error {
	defer p.db.Close()
	return p.tx.Commit()
}

func (p *Postgres) Migrate(name string, data io.Reader) error {
	if err := p.db.Ping(); err != nil {
		return fmt.Errorf("ping failed %s", err)
	}

	// Shame you can't stream statements to the driver as well.
	checksum := sha512.New()
	reader := io.TeeReader(data, checksum)
	statements, err := ioutil.ReadAll(reader)
	if err != nil {
		p.tx.Rollback()
		return err
	}

	rows, err := p.tx.Query(MigrateSQL, name)
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
			return fmt.Errorf("migration '%s' has been altered since it was run on %s", previous.name, previous.completed)
		}

		// TODO: Skip log.
		return nil
	}
	rows.Close()

	if _, err := p.tx.Exec(string(statements)); err != nil {
		// TODO: Not all errors are the same. These ones are problems with your migration.
		return err
	}

	if _, err = p.tx.Exec(`insert into schema_migrations (name, checksum) values ($1::text, $2::bytea)`, name, checksum.Sum(nil)); err != nil {
		return fmt.Errorf("schema_migrations insert %s", err)
	}

	return nil
}
