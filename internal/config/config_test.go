package config

import (
	"os"
	"path/filepath"
	"testing"
)

const baseYAML = `
server:
  port: 9000
concrnt:
  ccid: con1test
  domain: concrnt.example.com
  privateKey: placeholder
atproto:
  relays:
    - https://relay.example
  masterRotationKey: base-rotation
`

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestLoadSingleFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeFile(t, path, baseYAML)

	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Server.Port != 9000 || c.Concrnt.CCID != "con1test" {
		t.Fatalf("unexpected config: %+v", c)
	}
	if c.Atproto.PLCDirectory != "https://plc.directory" {
		t.Fatalf("default not applied: %+v", c.Atproto)
	}
	if c.Atproto.Proxy.AppviewDID != "did:web:api.bsky.app" || c.Atproto.Proxy.ChatDID != "did:web:api.bsky.chat" {
		t.Fatalf("proxy defaults not applied: %+v", c.Atproto.Proxy)
	}
}

func TestProxyAndSessionSecretOverride(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeFile(t, path, baseYAML+`
  sessionSecret: s3cret
  proxy:
    chatDid: did:web:chat.example
`)
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Atproto.SessionSecret != "s3cret" || c.Atproto.Proxy.ChatDID != "did:web:chat.example" || c.Atproto.Proxy.AppviewDID != "did:web:api.bsky.app" {
		t.Fatalf("override: %+v", c.Atproto)
	}
}

func TestLoadDirectoryMergesLaterFilesOverEarlier(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "config.yaml"), baseYAML)
	writeFile(t, filepath.Join(dir, "secret.yaml"), `
concrnt:
  privateKey: real-key
atproto:
  masterRotationKey: real-rotation
  keyEncryptionKey: real-kek
`)

	c, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if c.Concrnt.PrivateKey != "real-key" {
		t.Fatalf("secret did not override privateKey: %q", c.Concrnt.PrivateKey)
	}
	if c.Concrnt.CCID != "con1test" || c.Concrnt.Domain != "concrnt.example.com" {
		t.Fatalf("overlay clobbered sibling keys: %+v", c.Concrnt)
	}
	if c.Atproto.MasterRotationKey != "real-rotation" || c.Atproto.KeyEncryptionKey != "real-kek" {
		t.Fatalf("nested overlay not merged: %+v", c.Atproto)
	}
	if len(c.Atproto.Relays) != 1 || c.Atproto.Relays[0] != "https://relay.example" {
		t.Fatalf("overlay clobbered untouched slice: %+v", c.Atproto.Relays)
	}
	if c.Server.Port != 9000 {
		t.Fatalf("overlay clobbered untouched struct: %+v", c.Server)
	}
}

func TestLoadDirectorySkipsProjectedVolumeInternals(t *testing.T) {
	// A Kubernetes projected volume looks like:
	//   ..2026_01_01/config.yaml   (real files)
	//   ..data -> ..2026_01_01     (symlink to directory)
	//   config.yaml -> ..data/config.yaml
	dir := t.TempDir()
	real := filepath.Join(dir, "..2026_01_01")
	if err := os.Mkdir(real, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(real, "config.yaml"), baseYAML)
	writeFile(t, filepath.Join(real, "secret.yaml"), "concrnt:\n  privateKey: linked-key\n")
	if err := os.Symlink("..2026_01_01", filepath.Join(dir, "..data")); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"config.yaml", "secret.yaml"} {
		if err := os.Symlink(filepath.Join("..data", name), filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}

	c, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if c.Concrnt.PrivateKey != "linked-key" || c.Concrnt.CCID != "con1test" {
		t.Fatalf("unexpected config: %+v", c.Concrnt)
	}
}

func TestLoadRejectsMissingRequired(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "config.yaml"), "concrnt:\n  ccid: con1test\n  domain: x\n")
	if _, err := Load(dir); err == nil {
		t.Fatal("expected error for missing privateKey")
	}
	if _, err := Load(filepath.Join(dir, "none")); err == nil {
		t.Fatal("expected error for missing path")
	}
}
