package driver

import (
	"fmt"
	"io"
	"sort"
	"sync"
)

const (
	DefaultSchema    = "migrate"
	DefaultTableName = "schema_migrations"
)

// Logger is compatible with *slog.Logger.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

// Config holds configuration for drivers and migrate.
type Config struct {
	Schema    string
	TableName string
	Logger    Logger
	NameFunc  func(string) string
}

// Option configures a Config.
type Option func(*Config)

// WithSchema sets a custom schema name for the migrations table.
func WithSchema(schema string) Option {
	return func(c *Config) {
		c.Schema = schema
	}
}

// WithTableName sets a custom name for the migrations table.
func WithTableName(name string) Option {
	return func(c *Config) {
		c.TableName = name
	}
}

// WithLogger sets a custom logger.
func WithLogger(l Logger) Option {
	return func(c *Config) {
		c.Logger = l
	}
}

// WithNameFunc sets a custom function to transform file paths into migration names.
func WithNameFunc(f func(string) string) Option {
	return func(c *Config) {
		c.NameFunc = f
	}
}

type Driver func(dsn string, opts ...Option) (Migrator, error)

type Migrator interface {
	Begin() error
	Rollback() error
	Commit() error
	Migrate(name string, data io.Reader) error
}

var driversMutex sync.RWMutex
var drivers = make(map[string]Driver)

func Register(name string, driver Driver) {
	driversMutex.Lock()
	defer driversMutex.Unlock()

	if driver == nil {
		panic("register driver is nil")
	}
	if _, ok := drivers[name]; ok {
		panic(fmt.Sprintf("driver named '%s' already registered", name))
	}
	drivers[name] = driver
}

func Drivers() []string {
	driversMutex.RLock()
	defer driversMutex.RUnlock()
	var list []string
	for name := range drivers {
		list = append(list, name)
	}
	sort.Strings(list)
	return list
}

func New(driver, dsn string, opts ...Option) (Migrator, error) {
	driversMutex.RLock()
	d, ok := drivers[driver]
	driversMutex.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown driver %q (forgotten import?)", driver)
	}
	return d(dsn, opts...)
}
