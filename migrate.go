package migrate

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/shanna/migrate/driver"
)

const ModeExecutable os.FileMode = 0100

var logger = LogDefault()

type Migrate struct {
	driver string
	config string
}

func New(driver, config string) (*Migrate, error) {
	return &Migrate{driver, config}, nil
}

func (m *Migrate) Dir(dir string) error {
	migrator, err := driver.Begin(m.driver, m.config)
	if err != nil {
		return err
	}
	defer migrator.Rollback()

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, file := range files {
		path := filepath.Join(dir, file.Name())

		switch mode := file.Mode(); {
		case mode.IsRegular() && mode&ModeExecutable != 0:
			if err = fileExecute(migrator, path); err != nil {
				return err
			}
		case mode.IsRegular():
			if err := fileOpen(migrator, path); err != nil {
				return err
			}
		}
	}

	return migrator.Commit()
}

func fileExecute(migrator driver.Migrator, path string) error {
	cmd := exec.Command(path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	if err := migrator.Migrate(filepath.Base(path), stdout); err != nil {
		return err
	}

	return cmd.Wait()
}

func fileOpen(migrator driver.Migrator, path string) error {
	fh, err := os.Open(path)
	if err != nil {
		return err
	}
	return migrator.Migrate(filepath.Base(path), fh)
}
