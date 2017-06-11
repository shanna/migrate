package main

import (
	"flag"

	"github.com/facebookgo/flagenv"
)

type Config struct {
	Version bool
	Driver  string
	Dir     string
}

var defaults = Config{
	Version: false,
	Driver:  "postgres://localhost:5432?sslmode=disable",
	Dir:     ".",
}

func NewConfig() (*Config, error) {
	config := defaults
	flag.StringVar(&config.Driver, "driver", defaults.Driver, "ENV[MIGRATE_DRIVER] Migration driver.")
	flag.StringVar(&config.Dir, "dir", defaults.Dir, "ENV[MIGRATE_DIR] Migration directory.")
	flag.BoolVar(&config.Version, "version", false, "Version information.")

	flagenv.Prefix = "MIGRATE_"
	flagenv.Parse()
	flag.Parse()

	if config.Driver == defaults.Driver && flag.Arg(1) != "" {
		config.Driver = flag.Arg(1)
	}
	if config.Dir == defaults.Dir && flag.Arg(2) != "" {
		config.Dir = flag.Arg(2)
	}

	return &config, nil
}
