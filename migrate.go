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

// NameFunc transforms a file path into a migration name.
type NameFunc func(path string) string

// Logger is compatible with *slog.Logger.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

type noopLogger struct{}

func (noopLogger) Debug(string, ...any) {}
func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Error(string, ...any) {}

// Option configures a Migrate instance.
type Option func(*Migrate)

// WithNameFunc sets a custom function to transform file paths into migration names.
// Default is filepath.Base.
func WithNameFunc(f NameFunc) Option {
	return func(m *Migrate) {
		m.nameFunc = f
	}
}

// WithLogger sets a custom logger. Default is slog.Default().
func WithLogger(l Logger) Option {
	return func(m *Migrate) {
		m.logger = l
	}
}

type Migrate struct {
	migrator mdriver.Migrator
	nameFunc NameFunc
	logger   Logger
}

func New(driver, dsn string, opts ...Option) (*Migrate, error) {
	migrator, err := mdriver.New(driver, dsn)
	if err != nil {
		return nil, err
	}
	m := &Migrate{
		migrator: migrator,
		nameFunc: filepath.Base,
		logger:   slog.Default(),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m, nil
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
			m.logger.Debug("execute", "name", filepath.Base(path), "path", filepath.Dir(path))
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

			if err := m.execute(fh.Name(), m.nameFunc(path)); err != nil {
				return err
			}

		case mode.IsRegular():
			m.logger.Debug("read", "name", filepath.Base(path), "path", filepath.Dir(path))
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
			m.logger.Debug("execute", "name", filepath.Base(path), "path", filepath.Dir(path))
			if err = m.execute(path, m.nameFunc(path)); err != nil {
				return err
			}
		case mode.IsRegular():
			m.logger.Debug("read", "name", filepath.Base(path), "path", filepath.Dir(path))
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
