package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// LeveledLogger is a safe non-panicking logger.
type LeveledLogger interface {
	Debug(args ...interface{})
	Debugf(template string, args ...interface{})
	Debugw(msg string, keysAndValues ...interface{})

	Error(args ...interface{})
	Errorf(template string, args ...interface{})
	Errorw(msg string, keysAndValues ...interface{})

	Info(args ...interface{})
	Infof(template string, args ...interface{})
	Infow(msg string, keysAndValues ...interface{})

	Indent(name string) LeveledLogger
}

type leveledLogger struct {
	*zap.SugaredLogger
}

// Indent is an alias for Named, see https://godoc.org/go.uber.org/zap#Logger.Named
func (l leveledLogger) Indent(s string) LeveledLogger {
	newLogger := l.Named(s)
	return &leveledLogger{
		newLogger,
	}
}

// NewLeveledLogger instantiates a zap.SugaredLogger
func NewLeveledLogger(logLevel *string) LeveledLogger {
	atom := zap.NewAtomicLevelAt(zap.InfoLevel)
	if logLevel != nil {
		_ = (&atom).UnmarshalText([]byte(*logLevel))
	}

	highPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.ErrorLevel && lvl >= atom.Level()
	})
	lowPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl < zapcore.ErrorLevel && lvl >= atom.Level()
	})

	consoleEncoder := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())

	consoleDebugging := zapcore.Lock(os.Stdout)
	consoleErrors := zapcore.Lock(os.Stderr)

	core := zapcore.NewTee(
		zapcore.NewCore(consoleEncoder, consoleDebugging, lowPriority),
		zapcore.NewCore(consoleEncoder, consoleErrors, highPriority),
	)

	logger := zap.New(core)
	logger = logger.WithOptions(
		zap.AddCaller(),
		// zap.AddCallerSkip(1),
	)

	sugaredLogger := logger.Sugar()
	defer sugaredLogger.Sync()

	sugaredLogger.Info("logger constructed")
	sugaredLogger.Infof("Log level set to: %s", atom.Level())

	return &leveledLogger{sugaredLogger}
}
