package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/shanna/migrate"
	_ "github.com/shanna/migrate/driver/pq"
)

var (
	BuildTime = "Not built with -ldflags '-X main.BuildTime' ..."
	BuildGit  = "Not built with -ldflags '-X main.BuildGit ...'"
)

func main() {
	config, err := NewConfig()
	exitOnError(err)

	if config.Version {
		fmt.Printf("git:%s build:%s\n", BuildGit, BuildTime)
		os.Exit(0)
	}

	m, err := migrate.New(config.Driver, config.Config)
	exitOnError(err)

	if err := m.Dir(config.Arg(0)); err != nil {
		log.Printf("error\t%s\n", err)
		os.Exit(1)
	}
}

func exitOnError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n\n", err)
		flag.PrintDefaults()
		os.Exit(1)
	}
}
