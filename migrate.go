package migrate

import (
	"io/ioutil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/shanna/migrate/driver"
)

const ModeExecutable os.FileMode = 0100

type Migrate struct {
	migrator driver.Migrator
}

func New(config *url.URL) (*Migrate, error) {
	migrator, err := driver.New(config)
	if err != nil {
		return nil, err
	}
	return &Migrate{migrator}, nil
}

func (m *Migrate) Dir(dir string) error {
	if err := m.migrator.Begin(); err != nil {
		return err
	}
	defer m.migrator.Rollback()

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, file := range files {
		path := filepath.Join(dir, file.Name())

		switch mode := file.Mode(); {
		case mode.IsRegular() && mode&ModeExecutable != 0:
			log("execute\t%s\n", path)
			if err = fileExecute(m.migrator, path); err != nil {
				return err
			}
		case mode.IsRegular():
			log("read\t%s\n", path)
			if err := fileOpen(m.migrator, path); err != nil {
				return err
			}
		}
	}

	return m.migrator.Commit()
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
