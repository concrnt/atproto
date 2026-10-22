// Package mediatest generates small video fixtures with ffmpeg for tests.
// Tests that use it skip when ffmpeg is not installed.
package mediatest

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// Require skips the test when ffmpeg or ffprobe is not on PATH.
func Require(t testing.TB) {
	t.Helper()
	for _, bin := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("%s not installed", bin)
		}
	}
}

// Fixture renders a one second 320x240 test pattern with a sine tone using
// the given encoders and container, and returns the file's bytes. Pass
// audioCodec "" for a silent file. The encoder must exist in the local
// ffmpeg; mpeg4/aac/libx264 are in every common build.
func Fixture(t testing.TB, videoCodec, audioCodec, format string) []byte {
	t.Helper()
	Require(t)
	out := filepath.Join(t.TempDir(), "fixture."+format)
	args := []string{"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-f", "lavfi", "-i", "testsrc=duration=1:size=320x240:rate=10"}
	if audioCodec != "" {
		args = append(args, "-f", "lavfi", "-i", "sine=frequency=440:duration=1")
	}
	args = append(args, "-c:v", videoCodec, "-pix_fmt", "yuv420p")
	if audioCodec != "" {
		args = append(args, "-c:a", audioCodec)
	}
	args = append(args, "-shortest", "-f", format, out)
	cmd := exec.Command("ffmpeg", args...)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("ffmpeg could not render fixture (%v): %s", err, b)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
