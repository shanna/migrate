package main

import (
	"flag"
)

type Config struct {
	Version bool
	DSN     string
	Dir     string
}

var defaults = Config{
	Version: false,
	DSN:     "postgres://localhost:5432?sslmode=disable",
	Dir:     ".",
}

func NewConfig() (*Config, error) {
	config := defaults
	flag.StringVar(&config.DSN, "dsn", defaults.DSN, "Migration DSN.")
	flag.StringVar(&config.Dir, "dir", defaults.Dir, "Migration directory.")
	flag.Parse()

	if flag.Arg(0) != "" {
		config.DSN = flag.Arg(0)
	}
	if flag.Arg(1) != "" {
		config.Dir = flag.Arg(1)
	}

	return &config, nil
}
