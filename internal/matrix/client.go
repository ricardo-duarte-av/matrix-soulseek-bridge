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
	"time"

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

	// profileNames caches global-profile display names looked up by displayName,
	// so we don't hit GetProfile on every message from a user with no per-room
	// display name. Entries expire after profileNameTTL.
	profileMu    sync.Mutex
	profileNames map[id.UserID]profileNameEntry
}

// profileNameTTL is how long a cached global-profile display name stays valid.
const profileNameTTL = time.Hour

type profileNameEntry struct {
	name    string
	expires time.Time
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
		profileNames: make(map[id.UserID]profileNameEntry),
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

// displayName resolves a sender's display name. It prefers the per-room display
// name from the m.room.member event, then falls back to the user's global
// profile display name, and finally to the localpart of the user ID.
func (b *Bridge) displayName(ctx context.Context, userID id.UserID) string {
	member := b.as.BotIntent().Member(ctx, b.opts.RoomID, userID)
	if member != nil && member.Displayname != "" {
		return member.Displayname
	}
	if name := b.profileName(ctx, userID); name != "" {
		return name
	}
	return userID.Localpart()
}

// profileName returns the user's global-profile display name, caching results
// for profileNameTTL. A miss (error or empty name) is cached as "" so repeated
// messages from a user without a display name don't each trigger a lookup.
func (b *Bridge) profileName(ctx context.Context, userID id.UserID) string {
	b.profileMu.Lock()
	if entry, ok := b.profileNames[userID]; ok && time.Now().Before(entry.expires) {
		b.profileMu.Unlock()
		return entry.name
	}
	b.profileMu.Unlock()

	name := ""
	if profile, err := b.as.BotIntent().GetProfile(ctx, userID); err == nil {
		name = profile.DisplayName
	}

	b.profileMu.Lock()
	b.profileNames[userID] = profileNameEntry{name: name, expires: time.Now().Add(profileNameTTL)}
	b.profileMu.Unlock()
	return name
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

// ensureGhost registers the ghost, sets its display name, and joins it to the
// room the first time it is seen. The display name is set *before* joining so
// the join's m.room.member event carries it — otherwise clients fall back to
// the localpart (e.g. "soulseek_username") instead of the bare Soulseek name.
func (b *Bridge) ensureGhost(ctx context.Context, ghost id.UserID, displayName string) error {
	b.initMu.Lock()
	_, done := b.initialized[ghost]
	b.initMu.Unlock()

	intent := b.as.Intent(ghost)
	if !done {
		if err := intent.EnsureRegistered(ctx); err != nil {
			return fmt.Errorf("matrix: ghost %s register: %w", ghost, err)
		}
		if displayName != "" {
			if err := intent.SetDisplayName(ctx, displayName); err != nil {
				b.log.Warn("set ghost display name", "ghost", ghost, "err", err)
			}
		}
		b.markInitialized(ghost)
	}
	if err := intent.EnsureJoined(ctx, b.opts.RoomID); err != nil {
		return fmt.Errorf("matrix: ghost %s join: %w", ghost, err)
	}
	return nil
}

// markInitialized records that a ghost has been registered/named so the message
// path skips that work on subsequent messages.
func (b *Bridge) markInitialized(ghost id.UserID) {
	b.initMu.Lock()
	b.initialized[ghost] = struct{}{}
	b.initMu.Unlock()
}

// RebroadcastGhostNames repairs ghosts that joined the room before their display
// name was set: their m.room.member event still shows the localpart (e.g.
// "soulseek_username") rather than the bare Soulseek nickname. For each ghost
// the appservice owns, it copies the ghost's global profile display name (which
// the bridge keeps authoritative) into a fresh membership event when the two
// differ. It is best-effort: per-ghost failures are logged and skipped.
func (b *Bridge) RebroadcastGhostNames(ctx context.Context) error {
	members, err := b.as.BotIntent().JoinedMembers(ctx, b.opts.RoomID)
	if err != nil {
		return fmt.Errorf("matrix: list room members: %w", err)
	}
	botID := b.as.BotIntent().UserID
	repaired := 0
	for userID, member := range members.Joined {
		if userID == botID || !b.owns(userID) {
			continue
		}
		intent := b.as.Intent(userID)
		profile, err := intent.GetProfile(ctx, userID)
		if err != nil {
			b.log.Warn("rebroadcast: get ghost profile", "ghost", userID, "err", err)
			continue
		}
		// Already known to the bridge for this run; the message path won't
		// redo registration/naming for it.
		b.markInitialized(userID)
		if profile.DisplayName == "" || profile.DisplayName == member.DisplayName {
			continue // nothing to set, or the room already shows the right name
		}
		_, err = intent.SendStateEvent(ctx, b.opts.RoomID, event.StateMember, userID.String(), &event.MemberEventContent{
			Membership:  event.MembershipJoin,
			Displayname: profile.DisplayName,
			AvatarURL:   profile.AvatarURL.CUString(),
		})
		if err != nil {
			b.log.Warn("rebroadcast: update ghost name", "ghost", userID, "err", err)
			continue
		}
		b.log.Info("repaired ghost display name", "ghost", userID, "name", profile.DisplayName)
		repaired++
	}
	if repaired > 0 {
		b.log.Info("rebroadcast ghost display names", "repaired", repaired)
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
