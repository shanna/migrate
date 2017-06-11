package driver

import (
	"fmt"
	"io"
	"net/url"
	"sort"
	"sync"
)

type Driver func(config *url.URL) (Migrator, error)

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

func New(config *url.URL) (Migrator, error) {
	driversMutex.RLock()
	driver, ok := drivers[config.Scheme]
	driversMutex.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown driver %q (forgotten import?)", config.Scheme)
	}
	return driver(config)
}
