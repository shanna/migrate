package clickhouse

import (
	"context"
	"crypto/sha512"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/shanna/migrate/driver"
)

// migrationMutex prevents concurrent migrations within the same process.
// ClickHouse doesn't support traditional transactions for DDL, so external
// coordination is required for multi-process scenarios.
var migrationMutex sync.Mutex

func init() {
	driver.Register("clickhouse", New)
}

type migrate struct {
	name      string
	completed time.Time
	checksum  string
}

type ClickHouse struct {
	db        *sql.DB
	database  string
	tableName string
	locked    bool
}

func New(dsn string, opts ...driver.Option) (driver.Migrator, error) {
	config := &driver.Config{
		Schema:    driver.DefaultSchema,
		TableName: driver.DefaultTableName,
	}
	for _, opt := range opts {
		opt(config)
	}

	conn, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("sql open: %w", err)
	}

	if err := conn.Ping(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}

	c := &ClickHouse{
		db:        conn,
		database:  config.Schema,
		tableName: config.TableName,
	}

	return c, nil
}

func (c *ClickHouse) qualifiedTableName() string {
	return c.database + "." + c.tableName
}

func (c *ClickHouse) createDatabaseSQL() string {
	return fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, c.database)
}

func (c *ClickHouse) createTableSQL() string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  name String,
  checksum String,
  completed DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY name`, c.qualifiedTableName())
}

func (c *ClickHouse) selectMigrationSQL() string {
	return fmt.Sprintf(`
SELECT name, completed, checksum
FROM %s
WHERE name = ?;
`, c.qualifiedTableName())
}

func (c *ClickHouse) insertMigrationSQL() string {
	return fmt.Sprintf(`INSERT INTO %s (name, checksum) VALUES (?, ?)`, c.qualifiedTableName())
}

func (c *ClickHouse) Begin() error {
	migrationMutex.Lock()
	c.locked = true

	ctx := context.Background()

	if err := c.db.PingContext(ctx); err != nil {
		c.unlock()
		return fmt.Errorf("ping: %w", err)
	}

	// Setup creates database/table if needed.
	// ClickHouse doesn't support multi-statements or transactions for DDL,
	// so we execute setup statements separately.
	if _, err := c.db.ExecContext(ctx, c.createDatabaseSQL()); err != nil {
		c.unlock()
		return fmt.Errorf("setup database: %w", err)
	}

	if _, err := c.db.ExecContext(ctx, c.createTableSQL()); err != nil {
		c.unlock()
		return fmt.Errorf("setup table: %w", err)
	}

	return nil
}

func (c *ClickHouse) unlock() {
	if c.locked {
		c.locked = false
		migrationMutex.Unlock()
	}
}

func (c *ClickHouse) Rollback() error {
	defer c.db.Close()
	defer c.unlock()
	// ClickHouse doesn't support rolling back DDL statements.
	// Any migrations that ran are permanent.
	return fmt.Errorf("clickhouse does not support rollback; DDL changes are permanent")
}

func (c *ClickHouse) Commit() error {
	defer c.db.Close()
	defer c.unlock()
	// No-op: ClickHouse commits DDL immediately.
	return nil
}

func (c *ClickHouse) Migrate(name string, data io.Reader) error {
	log := slog.With("name", filepath.Base(name), "path", filepath.Dir(name), "driver", "clickhouse")
	ctx := context.Background()

	checksum := sha512.New()
	reader := io.TeeReader(data, checksum)
	statements, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}

	rows, err := c.db.QueryContext(ctx, c.selectMigrationSQL(), name)
	if err != nil {
		return fmt.Errorf("schema_migrations select previous: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		previous := migrate{}
		err = rows.Scan(&previous.name, &previous.completed, &previous.checksum)
		if err != nil {
			return fmt.Errorf("schema_migrations scan previous: %w", err)
		}
		if base64.StdEncoding.EncodeToString(checksum.Sum(nil)) != previous.checksum {
			return fmt.Errorf("%q has been altered since it was run on %s", previous.name, previous.completed)
		}

		log.Debug("skip", "reason", "already run", "completed", previous.completed)
		return nil
	}
	rows.Close()

	if _, err := c.db.ExecContext(ctx, string(statements)); err != nil {
		log.Error("error", "error", err, "sql", string(statements))
		return err
	}

	if _, err = c.db.ExecContext(ctx, c.insertMigrationSQL(), name, base64.StdEncoding.EncodeToString(checksum.Sum(nil))); err != nil {
		return fmt.Errorf("schema_migrations insert: %w", err)
	}

	log.Debug("commit")
	return nil
}
