package pq

import (
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
select name, completed, encode(checksum, 'hex') as checksum
from schema_migrations
where name = $1;
`

type migrate struct {
	name      string
	completed time.Time
	checksum  string
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
		return err
	}

	// Shame you can't stream statements to the driver as well.
	checksum := sha512.New()
	reader := io.TeeReader(data, checksum)
	statements, err := ioutil.ReadAll(reader)
	if err != nil {
		p.tx.Rollback() // TODO: What happens here,
		return err
	}

	// TODO: Should the same name but a different cheksum
	row := p.tx.QueryRow(MigrateSQL, name, checksum)
	if row != nil {
		previous := migrate{}
		if err := row.Scan(&previous.name, &previous.completed, &previous.checksum); err != nil {
			return err
		}

		// TODO: Derp. How do I compare the against the bytes I know are in hash.Hash somewhere?
		if fmt.Sprintf("%x", checksum) != previous.checksum {
			return fmt.Errorf("migration '%s' has been altered since it was run on %s.", previous.name, previous.completed)
		}

		// TODO: Skip log.
		return nil
	}

	if _, err := p.tx.Exec(string(statements)); err != nil {
		// TODO: Not all errors are the same. These ones are problems with your migration.
		return err
	}

	if _, err = p.tx.Exec(`insert into schema migrations (name, checksum) values ($1, $2)`, name, checksum); err != nil {
		return err
	}

	return nil
}
