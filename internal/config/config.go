// Package config loads and validates the bridge configuration (config.yaml).
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// defaultFlapSuppressionSeconds is the grace period used to absorb Soulseek
// leave/rejoin flaps when soulseek.flap_suppression_seconds is unset.
const defaultFlapSuppressionSeconds = 30

// Config is the top-level bridge configuration, mirroring sample.config.yaml.
type Config struct {
	Homeserver Homeserver `yaml:"homeserver"`
	Appservice Appservice `yaml:"appservice"`
	Matrix     Matrix     `yaml:"matrix"`
	Soulseek   Soulseek   `yaml:"soulseek"`
	Logging    Logging    `yaml:"logging"`
}

// Homeserver describes how to reach the Matrix homeserver.
type Homeserver struct {
	Address string `yaml:"address"`
	Domain  string `yaml:"domain"`
}

// Appservice holds the application-service identity and listener settings.
// These must match registration.yaml.
type Appservice struct {
	ID               string `yaml:"id"`
	Hostname         string `yaml:"hostname"`
	Port             int    `yaml:"port"`
	ASToken          string `yaml:"as_token"`
	HSToken          string `yaml:"hs_token"`
	BotUsername      string `yaml:"bot_username"`
	UsernameTemplate string `yaml:"username_template"`
}

// Matrix holds the bridged room.
type Matrix struct {
	RoomID string `yaml:"room_id"`
}

// Soulseek holds the Soulseek account and room to bridge.
type Soulseek struct {
	ServerAddress string `yaml:"server_address"`
	Username      string `yaml:"username"`
	Password      string `yaml:"password"`
	Room          string `yaml:"room"`

	// FlapSuppressionSeconds delays "left" announcements so that a quick
	// leave→rejoin (a client reconnecting after a network blip) is collapsed
	// into nothing instead of two noisy notices. nil applies the default;
	// 0 or negative disables suppression (announce leaves immediately).
	FlapSuppressionSeconds *int `yaml:"flap_suppression_seconds"`

	// AnnounceJoinsLeaves controls whether Soulseek join/leave events are
	// rendered into Matrix as bot notices. Soulseek clients re-join every room
	// on connect and leave them all on disconnect, so a flaky client produces a
	// stream of "X joined"/"X left" noise. Set to false to drop these entirely
	// and keep the Matrix timeline clean. nil applies the default (true).
	AnnounceJoinsLeaves *bool `yaml:"announce_joins_leaves"`
}

// Logging holds logging options.
type Logging struct {
	Level string `yaml:"level"`
}

// FormatGhostUserID builds the full Matrix user ID for a Soulseek nickname
// using UsernameTemplate and the homeserver domain.
func (c *Config) FormatGhostUserID(soulseekName string) string {
	localpart := strings.ReplaceAll(c.Appservice.UsernameTemplate, "{username}", sanitizeLocalpart(soulseekName))
	return fmt.Sprintf("@%s:%s", localpart, c.Homeserver.Domain)
}

// FlapSuppression returns the grace period for collapsing Soulseek
// leave/rejoin flaps. A return of 0 means suppression is disabled.
func (c *Config) FlapSuppression() time.Duration {
	s := defaultFlapSuppressionSeconds
	if c.Soulseek.FlapSuppressionSeconds != nil {
		s = *c.Soulseek.FlapSuppressionSeconds
	}
	if s <= 0 {
		return 0
	}
	return time.Duration(s) * time.Second
}

// AnnouncePresence reports whether Soulseek join/leave events should be
// mirrored into Matrix. Defaults to true when unset.
func (c *Config) AnnouncePresence() bool {
	if c.Soulseek.AnnounceJoinsLeaves == nil {
		return true
	}
	return *c.Soulseek.AnnounceJoinsLeaves
}

// BotUserID returns the bridge bot's full Matrix user ID.
func (c *Config) BotUserID() string {
	return fmt.Sprintf("@%s:%s", c.Appservice.BotUsername, c.Homeserver.Domain)
}

// Load reads, parses and validates the config file at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}

	var cfg Config
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) validate() error {
	missing := func(name, val string) error {
		if strings.TrimSpace(val) == "" {
			return fmt.Errorf("config: %s is required", name)
		}
		return nil
	}
	checks := []struct {
		name, val string
	}{
		{"homeserver.address", c.Homeserver.Address},
		{"homeserver.domain", c.Homeserver.Domain},
		{"appservice.id", c.Appservice.ID},
		{"appservice.as_token", c.Appservice.ASToken},
		{"appservice.hs_token", c.Appservice.HSToken},
		{"appservice.bot_username", c.Appservice.BotUsername},
		{"appservice.username_template", c.Appservice.UsernameTemplate},
		{"matrix.room_id", c.Matrix.RoomID},
		{"soulseek.server_address", c.Soulseek.ServerAddress},
		{"soulseek.username", c.Soulseek.Username},
		{"soulseek.password", c.Soulseek.Password},
		{"soulseek.room", c.Soulseek.Room},
	}
	for _, ch := range checks {
		if err := missing(ch.name, ch.val); err != nil {
			return err
		}
	}
	if c.Appservice.Port <= 0 || c.Appservice.Port > 65535 {
		return fmt.Errorf("config: appservice.port must be 1-65535, got %d", c.Appservice.Port)
	}
	if !strings.Contains(c.Appservice.UsernameTemplate, "{username}") {
		return fmt.Errorf("config: appservice.username_template must contain {username}")
	}
	if c.Logging.Level == "" {
		c.Logging.Level = "info"
	}
	return nil
}

// sanitizeLocalpart maps a Soulseek nickname to characters valid in a Matrix
// user-ID localpart. Anything outside [a-z0-9._=/-] is percent-ish escaped to
// an underscore-hex form so distinct names stay distinct.
func sanitizeLocalpart(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9',
			r == '.' || r == '_' || r == '-' || r == '=' || r == '/':
			b.WriteRune(r)
		default:
			fmt.Fprintf(&b, "=%02x", r)
		}
	}
	return b.String()
}
