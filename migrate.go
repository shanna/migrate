package migrate // import "github.com/shanna/migrate"

import (
	"fmt"
	"io/fs"
	"log/slog"
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
			slog.Debug("execute", "name", filepath.Base(path), "path", filepath.Dir(path))
			// TODO: Better way tow rite out the binary to a tempile?
			fh, err := os.CreateTemp(os.TempDir(), "migrate-*")
			if err != nil {
				return fmt.Errorf("mkdir temp: %w", err)
			}
			defer os.Remove(fh.Name())

			bytes, err := fs.ReadFile(fsys, path)
			if err != nil {
				return fmt.Errorf("read: %w", err)
			}
			if _, err := fh.Write(bytes); err != nil {
				return fmt.Errorf("write: %w", err)
			}
			fh.Close()

			if err := os.Chmod(fh.Name(), 0755); err != nil {
				return fmt.Errorf("chmod: %w", err)
			}

			if err := fileExecute(m.migrator, fh.Name()); err != nil {
				return err
			}

		case mode.IsRegular():
			slog.Debug("read", "name", filepath.Base(path), "path", filepath.Dir(path))
			fh, err := fsys.Open(path)
			if err != nil {
				return err
			}
			if err := m.migrator.Migrate(path, fh); err != nil {
				return err
			}
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
			return fmt.Errorf("stat: %w", err)
		}

		switch mode := info.Mode(); {
		case mode.IsRegular() && mode.Perm()&ModeExecutable != 0:
			slog.Debug("execute", "name", filepath.Base(path), "path", filepath.Dir(path))
			if err = fileExecute(m.migrator, path); err != nil {
				return err
			}
		case mode.IsRegular():
			slog.Debug("read", "name", filepath.Base(path), "path", filepath.Dir(path))
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

	if err := migrator.Migrate(path, stdout); err != nil {
		return err
	}

	return cmd.Wait()
}

func fileOpen(migrator mdriver.Migrator, path string) error {
	fh, err := os.Open(path)
	if err != nil {
		return err
	}
	return migrator.Migrate(path, fh)
}
