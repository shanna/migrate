package main

import (
	"flag"
	"fmt"
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

	/*
		if args := len(os.Args); args != 1 {
			exitOnError(fmt.Errorf("expected exactly 1 argument (migration directory) but got %d", args))
		}
	*/
	if err := m.Dir(config.Arg(0)); err != nil {
		fmt.Println(err)
	}
}

func exitOnError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n\n", err)
		flag.PrintDefaults()
		os.Exit(1)
	}
}
