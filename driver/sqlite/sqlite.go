package postgres

import (
	"crypto/sha512"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/shanna/migrate/driver"
	_ "modernc.org/sqlite"
)

func init() {
	driver.Register("sqlite", New)
}

type migrate struct {
	name      string
	completed time.Time
	checksum  string
}

type Sqlite struct {
	db        *sql.DB
	logger    driver.Logger
	schema    string
	tableName string
	inTx      bool
}

func New(dsn string, opts ...driver.Option) (driver.Migrator, error) {
	config := &driver.Config{
		Schema:    driver.DefaultSchema,
		TableName: driver.DefaultTableName,
		Logger:    slog.Default(),
	}
	for _, opt := range opts {
		opt(config)
	}

	conn, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sql open: %w", err)
	}

	s := &Sqlite{
		db:        conn,
		logger:    config.Logger,
		schema:    config.Schema,
		tableName: config.TableName,
	}

	return s, nil
}

// qualifiedTableName returns schema_tableName since SQLite doesn't support schemas.
func (s *Sqlite) qualifiedTableName() string {
	return s.schema + "_" + s.tableName
}

func (s *Sqlite) setupSQL() string {
	return fmt.Sprintf(`
create table if not exists %s (
  name text not null,
  checksum text not null,
  completed datetime not null default current_timestamp,
  unique(name, checksum)
);
`, s.qualifiedTableName())
}

func (s *Sqlite) selectMigrationSQL() string {
	return fmt.Sprintf(`
select name, completed, checksum
from %s
where name = ?;
`, s.qualifiedTableName())
}

func (s *Sqlite) insertMigrationSQL() string {
	return fmt.Sprintf(`insert into %s (name, checksum) values (?, ?)`, s.qualifiedTableName())
}

func (s *Sqlite) Begin() error {
	// Use EXCLUSIVE transaction to prevent concurrent migrations.
	if _, err := s.db.Exec("BEGIN EXCLUSIVE"); err != nil {
		return fmt.Errorf("begin exclusive: %w", err)
	}
	s.inTx = true

	// Ensure migration table exists (idempotent).
	if _, err := s.db.Exec(s.setupSQL()); err != nil {
		s.db.Exec("ROLLBACK")
		s.inTx = false
		return fmt.Errorf("setup: %w", err)
	}

	return nil
}

func (s *Sqlite) Rollback() error {
	defer s.db.Close()
	if s.inTx {
		s.db.Exec("ROLLBACK")
		s.inTx = false
	}
	return nil
}

func (s *Sqlite) Commit() error {
	defer s.db.Close()
	if s.inTx {
		if _, err := s.db.Exec("COMMIT"); err != nil {
			return err
		}
		s.inTx = false
	}
	return nil
}

func (s *Sqlite) Migrate(name string, data io.Reader) error {
	// Shame you can't stream statements to the driver as well.
	checksum := sha512.New()
	reader := io.TeeReader(data, checksum)
	statements, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}

	rows, err := s.db.Query(s.selectMigrationSQL(), name)
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

		s.logger.Debug(fmt.Sprintf("migrate skip %s", name), "driver", "sqlite", "completed", previous.completed)
		return nil
	}
	rows.Close()

	if _, err := s.db.Exec(string(statements)); err != nil {
		s.logger.Error(fmt.Sprintf("migrate error %s", name), "driver", "sqlite", "error", err, "sql", string(statements))
		return err
	}

	if _, err = s.db.Exec(s.insertMigrationSQL(), name, base64.StdEncoding.EncodeToString(checksum.Sum(nil))); err != nil {
		return fmt.Errorf("schema_migrations insert %s", err)
	}

	s.logger.Debug(fmt.Sprintf("migrate %s", name), "driver", "sqlite")
	return nil
}
