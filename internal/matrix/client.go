// Package matrix wraps mautrix-go's appservice support for the bridge. It runs
// the appservice HTTP listener, surfaces real users' room messages to the
// bridge, and lets the bridge speak into the room as "ghost" users (one Matrix
// user per Soulseek nickname) or as the bridge bot.
package matrix

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sync"

	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// Kind classifies an incoming Matrix message so the bridge can decide how to
// render it on the Soulseek side (text is echoed verbatim; media becomes a
// "sent an image/…" placeholder).
type Kind int

const (
	KindText Kind = iota
	KindEmote
	KindNotice
	KindImage
	KindVideo
	KindAudio
	KindFile
	KindLocation
	KindOther
)

// Incoming is a message from a real Matrix user in the bridged room.
type Incoming struct {
	Sender     id.UserID
	SenderName string // display name if known, else localpart
	Kind       Kind
	// Body is the text for text/emote/notice messages. For media it holds the
	// filename/caption the client provided (may be empty).
	Body string
}

// Handlers receive events from Matrix. Handlers may be nil.
type Handlers struct {
	OnMessage func(ctx context.Context, msg Incoming)
}

// Options configures the Matrix side.
type Options struct {
	HomeserverURL    string
	HomeserverDomain string
	Hostname         string
	Port             uint16
	Registration     *appservice.Registration
	RoomID           id.RoomID
	BotUsername      string
	Logger           *slog.Logger
}

// Bridge is the Matrix half of the bridge.
type Bridge struct {
	opts     Options
	log      *slog.Logger
	handlers Handlers

	as *appservice.AppService
	ep *appservice.EventProcessor

	// ownedRegexes matches user IDs the appservice controls (ghosts + bot), so
	// we can ignore our own echoed events and avoid loops.
	ownedRegexes []*regexp.Regexp

	// initialized tracks ghost user IDs we've already registered/joined/named.
	initMu      sync.Mutex
	initialized map[id.UserID]struct{}
}

// New constructs the Matrix bridge from options and registration.
func New(opts Options, handlers Handlers) (*Bridge, error) {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.Registration == nil {
		return nil, fmt.Errorf("matrix: registration is required")
	}

	as, err := appservice.CreateFull(appservice.CreateOpts{
		Registration:     opts.Registration,
		HomeserverDomain: opts.HomeserverDomain,
		HomeserverURL:    opts.HomeserverURL,
		HostConfig: appservice.HostConfig{
			Hostname: opts.Hostname,
			Port:     opts.Port,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("matrix: create appservice: %w", err)
	}

	owned := make([]*regexp.Regexp, 0, len(opts.Registration.Namespaces.UserIDs))
	for _, ns := range opts.Registration.Namespaces.UserIDs {
		re, err := regexp.Compile(ns.Regex)
		if err != nil {
			return nil, fmt.Errorf("matrix: bad user namespace regex %q: %w", ns.Regex, err)
		}
		owned = append(owned, re)
	}

	b := &Bridge{
		opts:         opts,
		log:          opts.Logger.With("component", "matrix"),
		handlers:     handlers,
		as:           as,
		ep:           appservice.NewEventProcessor(as),
		ownedRegexes: owned,
		initialized:  make(map[id.UserID]struct{}),
	}
	b.ep.On(event.EventMessage, b.handleMessage)
	return b, nil
}

// Start launches the event processor and HTTP listener, then ensures the bot
// is joined to the bridged room. It returns once startup is underway; the
// listener runs until ctx is cancelled.
func (b *Bridge) Start(ctx context.Context) error {
	b.ep.Start(ctx)
	go b.as.Start()

	go func() {
		<-ctx.Done()
		b.ep.Stop()
		b.as.Stop()
	}()

	if err := b.as.BotIntent().EnsureJoined(ctx, b.opts.RoomID); err != nil {
		return fmt.Errorf("matrix: bot join room: %w", err)
	}
	b.log.Info("matrix appservice started", "room", b.opts.RoomID, "addr", b.as.Host.Address())
	return nil
}

// owns reports whether the user ID belongs to this appservice (a ghost or bot).
func (b *Bridge) owns(userID id.UserID) bool {
	s := userID.String()
	for _, re := range b.ownedRegexes {
		if re.MatchString(s) {
			return true
		}
	}
	return false
}

// handleMessage processes m.room.message events from real users in the room.
func (b *Bridge) handleMessage(ctx context.Context, evt *event.Event) {
	if evt.RoomID != b.opts.RoomID {
		return
	}
	if b.owns(evt.Sender) {
		return // our own ghost/bot echo — ignore to avoid loops
	}

	content := evt.Content.AsMessage()
	if content == nil {
		return
	}
	if b.handlers.OnMessage == nil {
		return
	}

	msg := Incoming{
		Sender:     evt.Sender,
		SenderName: b.displayName(ctx, evt.Sender),
		Kind:       kindFromMsgType(content.MsgType),
		Body:       content.Body,
	}
	b.handlers.OnMessage(ctx, msg)
}

// displayName resolves a sender's room display name, falling back to the
// localpart of the user ID.
func (b *Bridge) displayName(ctx context.Context, userID id.UserID) string {
	member := b.as.BotIntent().Member(ctx, b.opts.RoomID, userID)
	if member != nil && member.Displayname != "" {
		return member.Displayname
	}
	return userID.Localpart()
}

// SendAsGhost ensures the ghost user exists, is joined to the room, and has the
// given display name, then sends a plain-text message as that ghost.
func (b *Bridge) SendAsGhost(ctx context.Context, ghost id.UserID, displayName, text string) error {
	intent := b.as.Intent(ghost)
	if err := b.ensureGhost(ctx, ghost, displayName); err != nil {
		return err
	}
	if _, err := intent.SendText(ctx, b.opts.RoomID, text); err != nil {
		return fmt.Errorf("matrix: send as %s: %w", ghost, err)
	}
	return nil
}

// SendBotNotice sends a notice from the bridge bot (used for system messages
// such as join/leave announcements).
func (b *Bridge) SendBotNotice(ctx context.Context, text string) error {
	if _, err := b.as.BotIntent().SendNotice(ctx, b.opts.RoomID, text); err != nil {
		return fmt.Errorf("matrix: bot notice: %w", err)
	}
	return nil
}

// ensureGhost registers the ghost, joins it to the room, and sets its display
// name the first time it is seen.
func (b *Bridge) ensureGhost(ctx context.Context, ghost id.UserID, displayName string) error {
	b.initMu.Lock()
	_, done := b.initialized[ghost]
	b.initMu.Unlock()

	intent := b.as.Intent(ghost)
	if err := intent.EnsureJoined(ctx, b.opts.RoomID); err != nil {
		return fmt.Errorf("matrix: ghost %s join: %w", ghost, err)
	}
	if !done {
		if displayName != "" {
			if err := intent.SetDisplayName(ctx, displayName); err != nil {
				b.log.Warn("set ghost display name", "ghost", ghost, "err", err)
			}
		}
		b.initMu.Lock()
		b.initialized[ghost] = struct{}{}
		b.initMu.Unlock()
	}
	return nil
}

func kindFromMsgType(t event.MessageType) Kind {
	switch t {
	case event.MsgText:
		return KindText
	case event.MsgEmote:
		return KindEmote
	case event.MsgNotice:
		return KindNotice
	case event.MsgImage:
		return KindImage
	case event.MsgVideo:
		return KindVideo
	case event.MsgAudio:
		return KindAudio
	case event.MsgFile:
		return KindFile
	case event.MsgLocation:
		return KindLocation
	default:
		return KindOther
	}
}
