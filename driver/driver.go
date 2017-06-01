package driver

import (
	"fmt"
	"io"
	"sort"
	"sync"
)

type Driver interface {
	Begin(config string) (Migrator, error)
}

type Migrator interface {
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

func Begin(driver, config string) (Migrator, error) {
	driversMutex.RLock()
	d, ok := drivers[driver]
	driversMutex.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown driver %q (forgotten import?)", driver)
	}
	return d.Begin(config)
}
