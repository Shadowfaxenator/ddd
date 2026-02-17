package prettylog

import (
	"log/slog"
	"os"
	"time"

	"github.com/lmittmann/tint"
)

func NewDefault() *slog.Logger {
	w := os.Stderr
	var handler slog.Handler
	handler = tint.NewHandler(w, &tint.Options{
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			switch attr.Key {
			case slog.MessageKey:
				return tint.Attr(4, attr)
			}

			return attr
		},
		Level:      slog.LevelDebug,
		TimeFormat: time.Kitchen,
	})

	return slog.New(handler)
}
