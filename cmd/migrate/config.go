package main

import (
	"flag"

	"github.com/facebookgo/flagenv"
)

type Config struct {
	Version bool
	Driver  string
	Config  string
}

var defaults = Config{
	Version: false,
	Driver:  "postgres",
	Config:  "postgres://localhost:5432?sslmode=disable",
}

func NewConfig() (*Config, error) {
	config := defaults

	flag.StringVar(&config.Driver, "driver", defaults.Driver, "ENV[MIGRATE_DRIVER] Migration driver.")
	flag.StringVar(&config.Config, "config", defaults.Driver, "ENV[MIGRATE_CONFIG] Migration driver config.")

	flag.BoolVar(&config.Version, "version", false, "Version information.")

	flagenv.Prefix = "MIGRATE_"
	flagenv.Parse()
	flag.Parse()

	return &config, nil
}

func (c Config) Arg(i int) string {
	return flag.Arg(i)
}
