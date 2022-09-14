package postgres

import (
	"context"
	"crypto/sha512"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net/url"
	"time"

	"github.com/shanna/migrate/driver"
	_ "modernc.org/sqlite"
)

func init() {
	driver.Register("sqlite", New)
}

const SetupSQL = `
create table if not exists schema_migrations (
  name text not null,
  checksum text not null,
  completed datetime not null default current_timestamp,
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

type Sqlite struct {
	db *sql.DB
	tx *sql.Tx
}

func New(config *url.URL) (driver.Migrator, error) {
	conn, err := sql.Open("sqlite", config.String())
	if err != nil {
		return nil, err
	}

	s := &Sqlite{
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

	s.db = conn

	return s, nil
}

func (s *Sqlite) Begin() error {
	ctx := context.TODO()

	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	s.tx = transaction
	return nil
}

func (s *Sqlite) Rollback() error {
	defer s.db.Close()
	return s.tx.Commit()
}

func (s *Sqlite) Commit() error {
	defer s.db.Close()
	return s.tx.Commit()
}

func (s *Sqlite) Migrate(name string, data io.Reader) error {
	// Shame you can't stream statements to the driver as well.
	checksum := sha512.New()
	reader := io.TeeReader(data, checksum)
	statements, err := io.ReadAll(reader)
	if err != nil {
		s.tx.Rollback()
		return fmt.Errorf("read %s", err)
	}

	rows, err := s.tx.Query(MigrateSQL, name)
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
			return fmt.Errorf("migration '%s' has been altered since it was run on %s", previous.name, previous.completed)
		}

		// TODO: Skip log.
		return nil
	}
	rows.Close()

	if _, err := s.tx.Exec(string(statements)); err != nil {
		log.Println(string(statements))
		log.Println(err.Error())
		return err
	}

	if _, err = s.tx.Exec(`insert into schema_migrations (name, checksum) values (?, ?)`, name, base64.StdEncoding.EncodeToString(checksum.Sum(nil))); err != nil {
		return fmt.Errorf("schema_migrations insert %s", err)
	}

	return nil
}
