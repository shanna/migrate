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

// Re-export options from driver package for convenience.
var (
	WithSchema    = mdriver.WithSchema
	WithTableName = mdriver.WithTableName
	WithLogger    = mdriver.WithLogger
	WithNameFunc  = mdriver.WithNameFunc
)

// Alias types from driver package.
type (
	Option = mdriver.Option
	Logger = mdriver.Logger
)

type Migrate struct {
	migrator mdriver.Migrator
	nameFunc func(string) string
	logger   Logger
}

func New(driver, dsn string, opts ...Option) (*Migrate, error) {
	// Build config with defaults to extract logger/nameFunc.
	config := &mdriver.Config{
		Schema:    mdriver.DefaultSchema,
		TableName: mdriver.DefaultTableName,
		Logger:    slog.Default(),
		NameFunc:  filepath.Base,
	}
	for _, opt := range opts {
		opt(config)
	}

	migrator, err := mdriver.New(driver, dsn, opts...)
	if err != nil {
		return nil, err
	}

	return &Migrate{
		migrator: migrator,
		nameFunc: config.NameFunc,
		logger:   config.Logger,
	}, nil
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
			m.logger.Debug(fmt.Sprintf("migrate execute %s", path))
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

			if err := m.execute(fh.Name(), m.nameFunc(path)); err != nil {
				return err
			}

		case mode.IsRegular():
			m.logger.Debug(fmt.Sprintf("migrate read %s", path))
			fh, err := fsys.Open(path)
			if err != nil {
				return err
			}
			if err := m.migrator.Migrate(m.nameFunc(path), fh); err != nil {
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
			m.logger.Debug(fmt.Sprintf("migrate execute %s", path))
			if err = m.execute(path, m.nameFunc(path)); err != nil {
				return err
			}
		case mode.IsRegular():
			m.logger.Debug(fmt.Sprintf("migrate read %s", path))
			if err := m.open(path); err != nil {
				return err
			}
		}
	}

	return m.migrator.Commit()
}

func (m *Migrate) execute(execPath, name string) error {
	cmd := exec.Command(execPath)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	if err := m.migrator.Migrate(name, stdout); err != nil {
		return err
	}

	return cmd.Wait()
}

func (m *Migrate) open(path string) error {
	fh, err := os.Open(path)
	if err != nil {
		return err
	}
	return m.migrator.Migrate(m.nameFunc(path), fh)
}
