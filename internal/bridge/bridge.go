// Package bridge wires the Matrix and Soulseek halves together: it forwards
// real Soulseek room messages into Matrix as ghost users, and echoes Matrix
// messages into the Soulseek room as the bridge account, prefixed with "[M]".
// Things Matrix supports but Soulseek cannot represent (images, video, files,
// …) are rendered as a textual placeholder on the Soulseek side.
package bridge

import (
	"context"
	"fmt"
	"sync"
	"time"

	"log/slog"

	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/id"

	"github.com/ricardo-duarte-av/matrix-soulseek-bridge/internal/config"
	"github.com/ricardo-duarte-av/matrix-soulseek-bridge/internal/matrix"
	"github.com/ricardo-duarte-av/matrix-soulseek-bridge/internal/soulseek"
)

// reconnectBackoff bounds the Soulseek reconnect delay.
const (
	reconnectMin = 2 * time.Second
	reconnectMax = 60 * time.Second
)

// Bridge owns both sides and the glue between them.
type Bridge struct {
	cfg *config.Config
	log *slog.Logger
	mx  *matrix.Bridge

	// ctx is the running context, captured in Run, so Soulseek read-loop
	// callbacks (which carry no context) can make Matrix API calls.
	ctx context.Context

	slskMu sync.Mutex
	slsk   *soulseek.Client

	// flapGrace is how long a "left" announcement is held back to absorb a
	// leave→rejoin flap. Zero disables suppression.
	flapGrace time.Duration
	// presenceMu guards pendingLeave.
	presenceMu sync.Mutex
	// pendingLeave maps a Soulseek username to the timer that will announce
	// their departure once the grace period elapses without a rejoin.
	pendingLeave map[string]*time.Timer
}

// New builds a Bridge from config and the loaded appservice registration.
func New(cfg *config.Config, reg *appservice.Registration, logger *slog.Logger) (*Bridge, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	b := &Bridge{
		cfg:          cfg,
		log:          logger,
		flapGrace:    cfg.FlapSuppression(),
		pendingLeave: make(map[string]*time.Timer),
	}

	mx, err := matrix.New(matrix.Options{
		HomeserverURL:    cfg.Homeserver.Address,
		HomeserverDomain: cfg.Homeserver.Domain,
		Hostname:         cfg.Appservice.Hostname,
		Port:             uint16(cfg.Appservice.Port),
		Registration:     reg,
		RoomID:           id.RoomID(cfg.Matrix.RoomID),
		BotUsername:      cfg.Appservice.BotUsername,
		Logger:           logger,
	}, matrix.Handlers{
		OnMessage: b.onMatrixMessage,
	})
	if err != nil {
		return nil, err
	}
	b.mx = mx
	return b, nil
}

// Run starts the Matrix appservice and the Soulseek connection loop, blocking
// until ctx is cancelled.
func (b *Bridge) Run(ctx context.Context) error {
	b.ctx = ctx
	if err := b.mx.Start(ctx); err != nil {
		return fmt.Errorf("start matrix: %w", err)
	}
	// Repair any ghosts that joined in a previous run before their display name
	// was set, so they show the bare Soulseek nickname instead of the localpart.
	if err := b.mx.RebroadcastGhostNames(ctx); err != nil {
		b.log.Warn("rebroadcast ghost display names failed", "err", err)
	}
	b.runSoulseek(ctx)
	return ctx.Err()
}

// runSoulseek maintains a Soulseek connection, reconnecting with backoff until
// ctx is cancelled.
func (b *Bridge) runSoulseek(ctx context.Context) {
	backoff := reconnectMin
	for ctx.Err() == nil {
		client := soulseek.New(soulseek.Options{
			ServerAddress: b.cfg.Soulseek.ServerAddress,
			Username:      b.cfg.Soulseek.Username,
			Password:      b.cfg.Soulseek.Password,
			Room:          b.cfg.Soulseek.Room,
			Logger:        b.log,
		}, soulseek.Handlers{
			OnRoomMessage: b.onSoulseekMessage,
			OnUserJoined:  b.onSoulseekJoined,
			OnUserLeft:    b.onSoulseekLeft,
		})

		if err := client.Connect(ctx); err != nil {
			b.log.Error("soulseek connect failed", "err", err, "retry_in", backoff)
			if !sleep(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}

		b.setClient(client)
		backoff = reconnectMin // reset after a successful connect

		err := client.Listen(ctx)
		b.setClient(nil)
		_ = client.Close()

		if ctx.Err() != nil {
			return
		}
		b.log.Warn("soulseek disconnected, reconnecting", "err", err, "retry_in", backoff)
		if !sleep(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff)
	}
}

func (b *Bridge) setClient(c *soulseek.Client) {
	b.slskMu.Lock()
	b.slsk = c
	b.slskMu.Unlock()
}

// onMatrixMessage echoes a Matrix message into the Soulseek room.
func (b *Bridge) onMatrixMessage(ctx context.Context, msg matrix.Incoming) {
	text := formatForSoulseek(msg)
	if err := b.sayToSoulseek(text); err != nil {
		b.log.Warn("forward matrix->soulseek failed", "err", err, "sender", msg.Sender)
	}
}

// sayToSoulseek sends text to the Soulseek room using the current connection,
// if any.
func (b *Bridge) sayToSoulseek(text string) error {
	b.slskMu.Lock()
	client := b.slsk
	b.slskMu.Unlock()
	if client == nil {
		return fmt.Errorf("soulseek not connected")
	}
	return client.Say(text)
}

// onSoulseekMessage forwards a Soulseek room message into Matrix as a ghost.
func (b *Bridge) onSoulseekMessage(username, message string) {
	// Ignore our own echoed messages to prevent loops: anything sent by the
	// bridge account (including the "[M] …" lines we just sent) comes back here.
	if username == b.cfg.Soulseek.Username {
		return
	}
	ghost := id.UserID(b.cfg.FormatGhostUserID(username))
	if err := b.mx.SendAsGhost(b.ctx, ghost, username, message); err != nil {
		b.log.Warn("forward soulseek->matrix failed", "err", err, "user", username)
	}
}

func (b *Bridge) onSoulseekJoined(username string) {
	if username == b.cfg.Soulseek.Username || !b.cfg.AnnouncePresence() {
		return
	}

	// If a leave for this user is still within its grace window, the user is
	// merely reconnecting after a network blip: cancel the pending "left"
	// notice and stay silent — from Matrix's point of view they never left.
	b.presenceMu.Lock()
	t, pending := b.pendingLeave[username]
	if pending {
		delete(b.pendingLeave, username)
	}
	b.presenceMu.Unlock()
	if pending && t.Stop() {
		return
	}

	if err := b.mx.SendBotNotice(b.ctx, fmt.Sprintf("%s joined the Soulseek room", username)); err != nil {
		b.log.Debug("join notice failed", "err", err)
	}
}

func (b *Bridge) onSoulseekLeft(username string) {
	if username == b.cfg.Soulseek.Username || !b.cfg.AnnouncePresence() {
		return
	}

	// With suppression disabled, announce immediately.
	if b.flapGrace <= 0 {
		b.announceLeft(username)
		return
	}

	// Otherwise hold the announcement: if the user rejoins within the grace
	// period, onSoulseekJoined cancels this timer and nothing is sent.
	b.presenceMu.Lock()
	if _, exists := b.pendingLeave[username]; exists {
		b.presenceMu.Unlock()
		return
	}
	var t *time.Timer
	t = time.AfterFunc(b.flapGrace, func() {
		b.presenceMu.Lock()
		// Only fire if we're still the active timer for this user; a rejoin
		// (or a newer leave) would have replaced or removed us.
		if b.pendingLeave[username] != t {
			b.presenceMu.Unlock()
			return
		}
		delete(b.pendingLeave, username)
		b.presenceMu.Unlock()
		b.announceLeft(username)
	})
	b.pendingLeave[username] = t
	b.presenceMu.Unlock()
}

// announceLeft posts the "left the Soulseek room" notice to Matrix.
func (b *Bridge) announceLeft(username string) {
	if err := b.mx.SendBotNotice(b.ctx, fmt.Sprintf("%s left the Soulseek room", username)); err != nil {
		b.log.Debug("leave notice failed", "err", err)
	}
}

// formatForSoulseek renders a Matrix message as a single line for the Soulseek
// room. Text is echoed verbatim; media and other unsupported content become a
// human-readable placeholder.
func formatForSoulseek(msg matrix.Incoming) string {
	name := msg.SenderName
	switch msg.Kind {
	case matrix.KindText, matrix.KindNotice:
		return fmt.Sprintf("[M] %s: %s", name, msg.Body)
	case matrix.KindEmote:
		return fmt.Sprintf("[M] * %s %s", name, msg.Body)
	case matrix.KindImage:
		return fmt.Sprintf("[M] %s sent an image%s", name, suffix(msg.Body))
	case matrix.KindVideo:
		return fmt.Sprintf("[M] %s sent a video%s", name, suffix(msg.Body))
	case matrix.KindAudio:
		return fmt.Sprintf("[M] %s sent an audio clip%s", name, suffix(msg.Body))
	case matrix.KindFile:
		return fmt.Sprintf("[M] %s sent a file%s", name, suffix(msg.Body))
	case matrix.KindLocation:
		return fmt.Sprintf("[M] %s shared a location", name)
	default:
		return fmt.Sprintf("[M] %s sent a message", name)
	}
}

// suffix renders an optional " (filename)" tail when a body/filename is present.
func suffix(body string) string {
	if body == "" {
		return ""
	}
	return fmt.Sprintf(" (%s)", body)
}

// sleep waits for d or until ctx is cancelled. It returns false if ctx was
// cancelled.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > reconnectMax {
		return reconnectMax
	}
	return d
}
