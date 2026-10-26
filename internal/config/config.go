package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"

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

// Proxy names the services that authenticated XRPC calls from atproto
// clients (the official Bluesky app) are forwarded to when the request
// carries no `atproto-proxy` header. Endpoints are resolved from the DID
// documents, so only the DIDs are configured.
type Proxy struct {
	AppviewDID string `yaml:"appviewDid"`
	ChatDID    string `yaml:"chatDid"`
}

type Atproto struct {
	PLCDirectory      string   `yaml:"plcDirectory"`
	Relays            []string `yaml:"relays"`
	Jetstream         string   `yaml:"jetstream"`
	Appview           Appview  `yaml:"appview"`
	Proxy             Proxy    `yaml:"proxy"`
	DataDir           string   `yaml:"dataDir"`
	MasterRotationKey string   `yaml:"masterRotationKey"` // multibase or hex, PLC rotation key
	KeyEncryptionKey  string   `yaml:"keyEncryptionKey"`  // 32-byte hex, AES-GCM for signing keys at rest
	// SessionSecret signs the access/refresh JWTs issued to atproto clients
	// (app-password login). Optional: when empty it is derived from
	// keyEncryptionKey, so no extra secret has to be provisioned.
	SessionSecret            string `yaml:"sessionSecret"`
	DisableRelayNotify       bool   `yaml:"disableRelayNotify"`
	FirehoseRetentionHours   int    `yaml:"firehoseRetentionHours"`
	ActorCacheTTLMinutes     int    `yaml:"actorCacheTtlMinutes"`
	EntityReloadIntervalSecs int    `yaml:"entityReloadIntervalSeconds"`
	Media                    Media  `yaml:"media"`
}

// Media configures ffmpeg processing on blob ingest: videos are transcoded
// to H.264/AAC mp4, images re-encoded to size-capped JPEG/PNG with metadata
// stripped. ffmpeg/ffprobe are looked up on PATH by default; when either is
// missing (or DisableTranscode is set) only video/mp4 and the natively
// supported image types are accepted, unchecked apart from the image cap.
type Media struct {
	FFmpegPath              string `yaml:"ffmpegPath"`
	FFprobePath             string `yaml:"ffprobePath"`
	DisableTranscode        bool   `yaml:"disableTranscode"`
	TranscodeTimeoutSeconds int    `yaml:"transcodeTimeoutSeconds"`
	// ImageMaxBytes caps image blobs. 1,000,000 is the official app's target
	// and the historical limit; app.bsky.embed.images allows 2,000,000 today.
	ImageMaxBytes int64 `yaml:"imageMaxBytes"`
	// ImageMaxDimension caps the long side of re-encoded images in pixels.
	ImageMaxDimension int `yaml:"imageMaxDimension"`
	// ImageSourceMaxBytes caps image downloads when ffmpeg re-encodes them.
	ImageSourceMaxBytes int64 `yaml:"imageSourceMaxBytes"`
}

type Config struct {
	Server   Server   `yaml:"server"`
	Database Database `yaml:"database"`
	Redis    Redis    `yaml:"redis"`
	Concrnt  Concrnt  `yaml:"concrnt"`
	Atproto  Atproto  `yaml:"atproto"`
}

// Load reads the config from path. path may be a single YAML file, or a
// directory: every regular file in it is parsed in name order and deep-merged
// (later files override earlier ones, field by field, so a secret.yaml
// holding only `concrnt.privateKey` layers over a config.yaml). This is the
// layout a Kubernetes projected volume (ConfigMap + Secret) produces, and
// mirrors the concrnt core / ActivityPub bridge loaders.
func Load(path string) (*Config, error) {
	if path == "" {
		path = os.Getenv("CONFIG_PATH")
	}
	if path == "" {
		path = "/app/config.yaml"
	}

	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config %s: %w", path, err)
	}

	var c Config
	if info.IsDir() {
		if err := loadDir(path, &c); err != nil {
			return nil, err
		}
	} else {
		if err := loadFile(path, &c); err != nil {
			return nil, err
		}
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
	if c.Atproto.Proxy.AppviewDID == "" {
		c.Atproto.Proxy.AppviewDID = "did:web:api.bsky.app"
	}
	if c.Atproto.Proxy.ChatDID == "" {
		c.Atproto.Proxy.ChatDID = "did:web:api.bsky.chat"
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
	if c.Atproto.Media.FFmpegPath == "" {
		c.Atproto.Media.FFmpegPath = "ffmpeg"
	}
	if c.Atproto.Media.FFprobePath == "" {
		c.Atproto.Media.FFprobePath = "ffprobe"
	}
	if c.Atproto.Media.TranscodeTimeoutSeconds == 0 {
		c.Atproto.Media.TranscodeTimeoutSeconds = 600
	}
	if c.Atproto.Media.ImageMaxBytes == 0 {
		c.Atproto.Media.ImageMaxBytes = 1_000_000
	}
	if c.Atproto.Media.ImageMaxDimension == 0 {
		c.Atproto.Media.ImageMaxDimension = 2000
	}
	if c.Atproto.Media.ImageSourceMaxBytes == 0 {
		c.Atproto.Media.ImageSourceMaxBytes = 20 << 20
	}

	if c.Concrnt.CCID == "" || c.Concrnt.Domain == "" || c.Concrnt.PrivateKey == "" {
		return nil, fmt.Errorf("concrnt.ccid, concrnt.domain and concrnt.privateKey are required")
	}

	return &c, nil
}

func loadFile(path string, c *Config) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(b, c); err != nil {
		return fmt.Errorf("failed to parse config %s: %w", path, err)
	}
	return nil
}

func loadDir(dir string, c *Config) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read config directory %s: %w", dir, err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	loaded := 0
	for _, name := range names {
		entryPath := filepath.Join(dir, name)
		// os.Stat follows symlinks: a projected volume exposes its files as
		// symlinks into a `..data` directory, which must be skipped as a
		// directory, while the file symlinks themselves must be read.
		info, err := os.Stat(entryPath)
		if err != nil || info.IsDir() {
			continue
		}
		var layer Config
		if err := loadFile(entryPath, &layer); err != nil {
			return err
		}
		merge(reflect.ValueOf(c).Elem(), reflect.ValueOf(&layer).Elem())
		loaded++
	}
	if loaded == 0 {
		return fmt.Errorf("no config files found in %s", dir)
	}
	return nil
}

// merge copies every non-zero field of src into dst, descending into nested
// structs so that a partial overlay only replaces the keys it names.
func merge(dst, src reflect.Value) {
	for i := range dst.NumField() {
		d := dst.Field(i)
		s := src.Field(i)
		if !d.CanSet() {
			continue
		}
		if d.Kind() == reflect.Struct {
			merge(d, s)
			continue
		}
		if s.IsZero() {
			continue
		}
		d.Set(s)
	}
}
