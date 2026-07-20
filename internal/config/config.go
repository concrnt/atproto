package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Server struct {
	// Port serves everything: /cc-info, the management API, and the public
	// PDS face (/xrpc/*, firehose, well-known). The concrnt gateway proxies
	// all of it, so the bridge must not be exposed to the internet directly
	// (the management API trusts the gateway's cc-requester header).
	Port int `yaml:"port"`
}

type Database struct {
	URL string `yaml:"url"`
}

type Redis struct {
	URL string `yaml:"url"`
}

type Concrnt struct {
	CCID       string `yaml:"ccid"`
	Domain     string `yaml:"domain"`
	PrivateKey string `yaml:"privateKey"`
	// PostURLTemplate renders a web permalink for a concrnt message, used as
	// the external embed when a post exceeds Bluesky's grapheme limit.
	// Placeholders: {ccid}, {uri} (url-escaped cckv/ccfs uri)
	PostURLTemplate string `yaml:"postUrlTemplate"`
}

type Appview struct {
	URL string `yaml:"url"`
}

type Atproto struct {
	PLCDirectory             string   `yaml:"plcDirectory"`
	Relays                   []string `yaml:"relays"`
	Jetstream                string   `yaml:"jetstream"`
	Appview                  Appview  `yaml:"appview"`
	DataDir                  string   `yaml:"dataDir"`
	MasterRotationKey        string   `yaml:"masterRotationKey"` // multibase or hex, PLC rotation key
	KeyEncryptionKey         string   `yaml:"keyEncryptionKey"`  // 32-byte hex, AES-GCM for signing keys at rest
	DisableRelayNotify       bool     `yaml:"disableRelayNotify"`
	FirehoseRetentionHours   int      `yaml:"firehoseRetentionHours"`
	ActorCacheTTLMinutes     int      `yaml:"actorCacheTtlMinutes"`
	EntityReloadIntervalSecs int      `yaml:"entityReloadIntervalSeconds"`
}

type Config struct {
	Server   Server   `yaml:"server"`
	Database Database `yaml:"database"`
	Redis    Redis    `yaml:"redis"`
	Concrnt  Concrnt  `yaml:"concrnt"`
	Atproto  Atproto  `yaml:"atproto"`
}

func Load(path string) (*Config, error) {
	if path == "" {
		path = os.Getenv("CONFIG_PATH")
	}
	if path == "" {
		path = "/app/config.yaml"
	}

	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config %s: %w", path, err)
	}

	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("failed to parse config %s: %w", path, err)
	}

	if c.Server.Port == 0 {
		c.Server.Port = 8010
	}
	if c.Atproto.PLCDirectory == "" {
		c.Atproto.PLCDirectory = "https://plc.directory"
	}
	if len(c.Atproto.Relays) == 0 {
		c.Atproto.Relays = []string{"https://bsky.network"}
	}
	if c.Atproto.Jetstream == "" {
		c.Atproto.Jetstream = "jetstream2.us-east.bsky.network"
	}
	if c.Atproto.Appview.URL == "" {
		c.Atproto.Appview.URL = "https://public.api.bsky.app"
	}
	if c.Atproto.DataDir == "" {
		c.Atproto.DataDir = "/data"
	}
	if c.Atproto.FirehoseRetentionHours == 0 {
		c.Atproto.FirehoseRetentionHours = 72
	}
	if c.Atproto.ActorCacheTTLMinutes == 0 {
		c.Atproto.ActorCacheTTLMinutes = 60
	}
	if c.Atproto.EntityReloadIntervalSecs == 0 {
		c.Atproto.EntityReloadIntervalSecs = 60
	}

	if c.Concrnt.CCID == "" || c.Concrnt.Domain == "" || c.Concrnt.PrivateKey == "" {
		return nil, fmt.Errorf("concrnt.ccid, concrnt.domain and concrnt.privateKey are required")
	}

	return &c, nil
}
