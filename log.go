package migrate

import "go.uber.org/zap"

type Logger interface {
	Info(legacy string, kv ...interface{}) error
}

func LogDefault() Logger {
	zap, _ := LogZap()
	return zap
}

// Zap concrete implementation.
type logzap struct {
	zap   *zap.Logger
	sugar *zap.SugaredLogger
}

func LogZap() (Logger, error) {
	zap, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}
	return &logzap{zap, zap.Sugar()}, nil
}

func (z *logzap) Info(legacy string, kv ...interface{}) error {
	z.sugar.Infow(legacy, kv...)
	return nil
}
