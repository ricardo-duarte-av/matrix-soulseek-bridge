// Command matrix-soulseek-bridge bridges a Soulseek chat room and a Matrix room.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"maunium.net/go/mautrix/appservice"

	"github.com/ricardo-duarte-av/matrix-soulseek-bridge/internal/bridge"
	"github.com/ricardo-duarte-av/matrix-soulseek-bridge/internal/config"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to the bridge config file")
	registrationPath := flag.String("registration", "registration.yaml", "path to the appservice registration file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fatal("load config", err)
	}

	logger := newLogger(cfg.Logging.Level)

	reg, err := appservice.LoadRegistration(*registrationPath)
	if err != nil {
		fatal("load registration", err)
	}
	// Guard against config/registration drift: the tokens must agree or the
	// homeserver and bridge will fail to authenticate each other.
	if reg.AppToken != cfg.Appservice.ASToken || reg.ServerToken != cfg.Appservice.HSToken {
		fatal("config/registration mismatch", errTokenMismatch)
	}

	b, err := bridge.New(cfg, reg, logger)
	if err != nil {
		fatal("init bridge", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("starting matrix-soulseek-bridge")
	if err := b.Run(ctx); err != nil && ctx.Err() == nil {
		fatal("run", err)
	}
	logger.Info("shutdown complete")
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}

func fatal(msg string, err error) {
	slog.Error(msg, "err", err)
	os.Exit(1)
}

// errTokenMismatch is reported when config tokens differ from registration.
var errTokenMismatch = &configError{"as_token/hs_token in config.yaml must match registration.yaml"}

type configError struct{ msg string }

func (e *configError) Error() string { return e.msg }
