package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"

	"github.com/shanna/migrate"
	_ "github.com/shanna/migrate/driver/postgres"
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

	driver, err := url.Parse(config.DSN)
	exitOnError(err)

	migrator, err := migrate.New(driver.Scheme, config.DSN)
	exitOnError(err)

	if err := migrator.Dir(config.Dir); err != nil {
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
