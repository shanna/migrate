package migrate // import "github.com/shanna/migrate"

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"

	mdriver "github.com/shanna/migrate/driver"
)

const ModeExecutable os.FileMode = 0100

type Migrate struct {
	migrator mdriver.Migrator
}

func New(driver, dsn string) (*Migrate, error) {
	migrator, err := mdriver.New(driver, dsn)
	if err != nil {
		return nil, err
	}
	return &Migrate{migrator}, nil
}

func (m *Migrate) DirFS(fsys fs.FS, dir string) error {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return err
	}

	if err := m.migrator.Begin(); err != nil {
		return err
	}
	defer m.migrator.Rollback()

	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return err
		}

		path := filepath.Join(dir, entry.Name())

		switch mode := info.Mode(); {
		case mode.IsDir():
			continue
		case mode.IsRegular() && mode.Perm()&ModeExecutable != 0:
			log("execute\t%s\n", path)
			// TODO: Better way tow rite out the binary to a tempile?
			fh, err := os.CreateTemp(os.TempDir(), "migrate-*")
			if err != nil {
				return err
			}
			defer os.Remove(fh.Name())

			bytes, err := fs.ReadFile(fsys, path)
			if err != nil {
				return err
			}
			if _, err := fh.Write(bytes); err != nil {
				return err
			}
			fh.Close()

			if err := os.Chmod(fh.Name(), 0755); err != nil {
				return err
			}

			if err := fileExecute(m.migrator, fh.Name()); err != nil {
				return err
			}

		case mode.IsRegular():
			log("read\t%s\n", path)
			fh, err := fsys.Open(path)
			if err != nil {
				return err
			}
			m.migrator.Migrate(path, fh)
		}
	}

	return m.migrator.Commit()
}

func (m *Migrate) Dir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	if err := m.migrator.Begin(); err != nil {
		return err
	}
	defer m.migrator.Rollback()

	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())

		info, err := entry.Info()
		if err != nil {
			return err
		}

		switch mode := info.Mode(); {
		case mode.IsRegular() && mode.Perm()&ModeExecutable != 0:
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

func fileExecute(migrator mdriver.Migrator, path string) error {
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

func fileOpen(migrator mdriver.Migrator, path string) error {
	fh, err := os.Open(path)
	if err != nil {
		return err
	}
	return migrator.Migrate(filepath.Base(path), fh)
}
