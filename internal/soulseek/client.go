// Package soulseek implements a minimal Soulseek client focused on a single
// chat room. The upstream github.com/bh90210/soul library provides protocol
// message (de)serialization for the server connection but no high-level chat
// client, so we build a thin one here: connect, log in, join one room, read
// the message stream, and send room messages.
package soulseek

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/bh90210/soul/server"
)

// Handlers receive events from the Soulseek room. Any handler may be nil; nil
// handlers are simply ignored. Handlers are invoked from the read loop
// goroutine, so they should not block for long.
type Handlers struct {
	// OnRoomMessage fires for every chat message in the bridged room,
	// including messages echoed by our own account.
	OnRoomMessage func(username, message string)
	// OnUserJoined fires when a user joins the bridged room.
	OnUserJoined func(username string)
	// OnUserLeft fires when a user leaves the bridged room.
	OnUserLeft func(username string)
}

// Options configures a Client.
type Options struct {
	// ServerAddress is the Soulseek server, host:port.
	ServerAddress string
	// Username and Password authenticate the bridge's Soulseek account.
	Username string
	Password string
	// Room is the chat room to join and bridge.
	Room string
	// ListenPort is advertised to the server via SetListenPort. Peers would
	// use it to connect for transfers; for a chat-only bridge it is largely
	// cosmetic but the server expects clients to set one.
	ListenPort uint32
	// Logger is used for diagnostics. If nil, a no-op logger is used.
	Logger *slog.Logger
}

// Client is a single-session Soulseek chat client. A Client is not safe to
// reconnect; create a new one (or call Connect again after Close) per session.
// Say is safe to call concurrently with the read loop.
type Client struct {
	opts     Options
	log      *slog.Logger
	handlers Handlers

	conn     net.Conn
	writeMu  sync.Mutex
	loggedIn bool
}

// New creates a Client. Handlers may be set now or via SetHandlers before
// Connect/Listen.
func New(opts Options, handlers Handlers) *Client {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.ListenPort == 0 {
		opts.ListenPort = 2234
	}
	return &Client{
		opts:     opts,
		log:      opts.Logger.With("component", "soulseek"),
		handlers: handlers,
	}
}

// SetHandlers replaces the event handlers. Call before Listen.
func (c *Client) SetHandlers(h Handlers) { c.handlers = h }

// Connect dials the server, logs in, advertises a listen port, sets the user
// status to online, and joins the configured room. It blocks until login
// completes or fails.
func (c *Client) Connect(ctx context.Context) error {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", c.opts.ServerAddress)
	if err != nil {
		return fmt.Errorf("dial %s: %w", c.opts.ServerAddress, err)
	}
	c.conn = conn

	if err := c.login(); err != nil {
		_ = conn.Close()
		return err
	}

	// Best-effort post-login setup. Failures here are not fatal to chat.
	if err := send(c, server.SetListenPort{}, func(m server.SetListenPort) ([]byte, error) {
		return m.Serialize(c.opts.ListenPort)
	}); err != nil {
		c.log.Warn("set listen port failed", "err", err)
	}
	if err := send(c, server.SetStatus{}, func(m server.SetStatus) ([]byte, error) {
		return m.Serialize(server.StatusOnline)
	}); err != nil {
		c.log.Warn("set status failed", "err", err)
	}

	if err := send(c, server.JoinRoom{}, func(m server.JoinRoom) ([]byte, error) {
		return m.Serialize(c.opts.Room, false)
	}); err != nil {
		_ = conn.Close()
		return fmt.Errorf("join room %q: %w", c.opts.Room, err)
	}

	c.log.Info("connected to soulseek", "room", c.opts.Room, "user", c.opts.Username)
	return nil
}

// login sends the Login message and reads server messages until the login
// response arrives, returning an error on authentication failure.
func (c *Client) login() error {
	payload, err := server.Login{}.Serialize(c.opts.Username, c.opts.Password)
	if err != nil {
		return fmt.Errorf("serialize login: %w", err)
	}
	if _, err := server.MessageWrite(c.conn, payload); err != nil {
		return fmt.Errorf("write login: %w", err)
	}

	// Read until we see the login response. The server may interleave other
	// messages; we dispatch the few we care about and ignore the rest.
	for {
		buf, _, code, err := server.MessageRead(c.conn)
		if err != nil {
			return fmt.Errorf("read login response: %w", err)
		}
		if code != server.CodeLogin {
			c.dispatch(code, buf)
			continue
		}
		var resp server.Login
		if err := resp.Deserialize(buf); err != nil {
			return fmt.Errorf("login failed: %w", err)
		}
		c.loggedIn = true
		c.log.Debug("login ok", "greeting", resp.Greet)
		return nil
	}
}

// Listen runs the read loop until the connection drops or ctx is cancelled.
// It also sends periodic keepalive pings. Connect must have succeeded first.
func (c *Client) Listen(ctx context.Context) error {
	if c.conn == nil {
		return fmt.Errorf("soulseek: Listen called before Connect")
	}

	// Closing the connection on ctx cancellation unblocks the read below.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = c.conn.Close()
		case <-stop:
		}
	}()

	go c.keepalive(ctx, stop)

	for {
		buf, _, code, err := server.MessageRead(c.conn)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read: %w", err)
		}
		c.dispatch(code, buf)
	}
}

// keepalive sends a Ping periodically to keep the connection and any NAT
// mapping alive.
func (c *Client) keepalive(ctx context.Context, stop <-chan struct{}) {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-t.C:
			if err := send(c, server.Ping{}, func(m server.Ping) ([]byte, error) {
				return m.Serialize()
			}); err != nil {
				c.log.Debug("ping failed", "err", err)
			}
		}
	}
}

// dispatch decodes a server message and invokes the relevant handler. Unknown
// or uninteresting message codes are ignored.
func (c *Client) dispatch(code server.Code, buf io.Reader) {
	switch code {
	case server.CodeSayChatroom:
		var m server.SayChatroom
		if err := m.Deserialize(buf); err != nil {
			c.log.Warn("decode SayChatroom", "err", err)
			return
		}
		if m.Room != c.opts.Room {
			return
		}
		if c.handlers.OnRoomMessage != nil {
			c.handlers.OnRoomMessage(m.Username, m.Message)
		}

	case server.CodeUserJoinedRoom:
		var m server.UserJoinedRoom
		if err := m.Deserialize(buf); err != nil {
			c.log.Warn("decode UserJoinedRoom", "err", err)
			return
		}
		if m.Room != c.opts.Room {
			return
		}
		if c.handlers.OnUserJoined != nil {
			c.handlers.OnUserJoined(m.Username)
		}

	case server.CodeUserLeftRoom:
		var m server.UserLeftRoom
		if err := m.Deserialize(buf); err != nil {
			c.log.Warn("decode UserLeftRoom", "err", err)
			return
		}
		if m.Room != c.opts.Room {
			return
		}
		if c.handlers.OnUserLeft != nil {
			c.handlers.OnUserLeft(m.Username)
		}

	default:
		// Ignore everything else (room lists, peer addresses, privileges, etc.).
	}
}

// Say sends a chat message to the bridged room. Note the server echoes the
// message back as a SayChatroom event from our own username, so callers should
// expect their own messages to also arrive via OnRoomMessage.
func (c *Client) Say(message string) error {
	return send(c, server.SayChatroom{}, func(m server.SayChatroom) ([]byte, error) {
		return m.Serialize(c.opts.Room, message)
	})
}

// Username returns the bridge's Soulseek username (useful to filter our own
// echoed messages).
func (c *Client) Username() string { return c.opts.Username }

// Close terminates the connection.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// send serializes a message via the provided closure and writes it to the
// connection under the write lock. The generic helper keeps each call site to
// a single serialize expression while sharing locking and error wrapping.
func send[M any](c *Client, msg M, serialize func(M) ([]byte, error)) error {
	payload, err := serialize(msg)
	if err != nil {
		return fmt.Errorf("serialize: %w", err)
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.conn == nil {
		return fmt.Errorf("soulseek: not connected")
	}
	if _, err := server.MessageWrite(c.conn, payload); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	return nil
}
