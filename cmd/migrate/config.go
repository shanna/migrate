package main

import (
	"flag"
)

type Config struct {
	Version   bool
	DSN       string
	Dir       string
	Schema    string
	TableName string
}

var defaults = Config{
	Version:   false,
	DSN:       "postgres://localhost:5432?sslmode=disable",
	Dir:       ".",
	Schema:    "",
	TableName: "",
}

func NewConfig() (*Config, error) {
	config := defaults
	flag.StringVar(&config.DSN, "dsn", defaults.DSN, "Migration DSN.")
	flag.StringVar(&config.Dir, "dir", defaults.Dir, "Migration directory.")
	flag.StringVar(&config.Schema, "schema", defaults.Schema, "Schema name for migrations table (Postgres/DuckDB only).")
	flag.StringVar(&config.TableName, "table", defaults.TableName, "Custom name for migrations table.")
	flag.Parse()

	if flag.Arg(0) != "" {
		config.DSN = flag.Arg(0)
	}
	if flag.Arg(1) != "" {
		config.Dir = flag.Arg(1)
	}

	return &config, nil
}
