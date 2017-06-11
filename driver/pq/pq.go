package pq

import (
	"bytes"
	"crypto/sha512"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"time"

	_ "github.com/lib/pq"
	"github.com/shanna/migrate/driver"
)

func init() {
	driver.Register("pg", New)
	driver.Register("postgres", New)
	driver.Register("postgresql", New)
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
	config *url.URL
	db     *sql.DB
	tx     *sql.Tx
	// closed/mutex
}

func New(config *url.URL) (driver.Migrator, error) {
	connection, err := sql.Open("postgres", config.String())
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
	defer transaction.Rollback()
	if _, err = transaction.Exec(SetupSQL); err != nil {
		return nil, err
	}
	transaction.Commit()

	return &Postgres{db: connection, config: config}, nil
}

func (p *Postgres) Begin() error {
	if err := p.db.Ping(); err != nil {
		return fmt.Errorf("ping failed %s", err)
	}

	transaction, err := p.db.Begin()
	if err != nil {
		return err
	}

	p.tx = transaction
	return nil
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
