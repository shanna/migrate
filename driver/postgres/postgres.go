package postgres

import (
	"bytes"
	"context"
	"crypto/sha512"
	"fmt"
	"io"
	"log"
	"math"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
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
	db     *pgx.Conn
	tx     pgx.Tx
	// closed/mutex? The driver provides synchronization for calls but the pq implementation on its own isn't safe.
	error Error
}

type Error struct { // Common?
	Message    string
	File       string
	Line       int
	Near       string
	NearMarker int
}

func (e Error) Error() string {
	return fmt.Sprintf("%s\n\nError: %s\nFile:  %s\nLine:  %d\nNear:  %s\n       %s",
		e.Message,
		e.Message,
		e.File,
		e.Line,
		e.Near,
		strings.Repeat(" ", e.NearMarker)+"^",
	)
}

// Both pq and pgx don't return the original query in the error structure so in the short term
// dig it out of the logs.
func (p *Postgres) Log(_ context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
	err, ok := data["err"]
	if !ok {
		return
	}

	pgxErr, ok := err.(*pgconn.PgError)
	if !ok {
		return
	}
	position := int(pgxErr.Position - 1)

	sql, ok := data["sql"].(string)
	if !ok {
		return
	}

	length := len(sql)
	if length > math.MaxInt32 || position < 0 || position >= length {
		return
	}

	start := strings.LastIndex(sql[:position], "\n")
	if start == -1 {
		start = 0
	} else {
		start++
	}

	end := strings.Index(sql[position:], "\n")
	if end == -1 {
		end = length
	} else {
		end += position
	}

	p.error = Error{
		Message:    pgxErr.Message,
		Line:       strings.Count(sql[:start], "\n") + 1,
		Near:       sql[start:end],
		NearMarker: position - start,
	}
}

func New(config *url.URL) (driver.Migrator, error) {
	connConfig, err := pgx.ParseConfig(config.String())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	pg := &Postgres{}

	connConfig.LogLevel = pgx.LogLevelDebug
	connConfig.Logger = pg

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
	pg.config = config

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

	rows, err := p.tx.Query(ctx, MigrateSQL, name)
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

	if _, err := p.tx.Exec(ctx, string(statements)); err != nil {
		log.Printf(string(statements))
		log.Printf(err.Error())
		// TODO: Not all errors are the same. These ones are problems with your migration.
		if _, ok := err.(*pgconn.PgError); ok {
			p.error.File = name
			return p.error
		}
		return err
	}

	if _, err = p.tx.Exec(ctx, `insert into schema_migrations (name, checksum) values ($1::text, $2::bytea)`, name, checksum.Sum(nil)); err != nil {
		return fmt.Errorf("schema_migrations insert %s", err)
	}

	return nil
}
