package logger

import (
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func New(json bool, isoTime bool) logr.Logger {

	zc := zap.NewProductionConfig()
	if json {
		zc.Encoding = "json"
	} else {
		zc.Encoding = "console"
	}
	if isoTime {
		zc.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	}
	zc.Level = zap.NewAtomicLevelAt(zapcore.Level(-2))
	z, _ := zc.Build()
	return zapr.NewLogger(z)
}
