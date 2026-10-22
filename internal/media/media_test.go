package media

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/concrnt/atproto/internal/media/mediatest"
)

func newTranscoder(t *testing.T) *Transcoder {
	mediatest.Require(t)
	tc, err := New("", "", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return tc
}

func write(t *testing.T, name string, data []byte) string {
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestNewMissingBinary(t *testing.T) {
	if _, err := New("/nonexistent/ffmpeg-xyz", "", 0); err == nil {
		t.Fatal("expected error for missing ffmpeg")
	}
}

func TestProbeAndNeedsTranscode(t *testing.T) {
	tc := newTranscoder(t)
	ctx := context.Background()

	cases := []struct {
		name, vcodec, acodec, format, mime string
		want                               bool
	}{
		{"mpeg4 mov", "mpeg4", "aac", "mov", "video/quicktime", true},
		{"mpeg4 in mp4", "mpeg4", "aac", "mp4", "video/mp4", true},
		{"h264 mp4 with mp3", "libx264", "libmp3lame", "mp4", "video/mp4", true},
		{"h264 aac mp4", "libx264", "aac", "mp4", "video/mp4", false},
		{"h264 silent mp4", "libx264", "", "mp4", "video/mp4", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := write(t, "in."+c.format, mediatest.Fixture(t, c.vcodec, c.acodec, c.format))
			info, err := tc.Probe(ctx, p)
			if err != nil {
				t.Fatal(err)
			}
			if info.Width != 320 || info.Height != 240 {
				t.Fatalf("dimensions = %dx%d", info.Width, info.Height)
			}
			if (c.acodec == "") != (info.AudioCodec == "") {
				t.Fatalf("audio codec = %q for fixture acodec %q", info.AudioCodec, c.acodec)
			}
			if got := NeedsTranscode(c.mime, info); got != c.want {
				t.Fatalf("NeedsTranscode = %v, want %v (info %+v)", got, c.want, info)
			}
		})
	}
}

func TestProbeRejectsNonVideo(t *testing.T) {
	tc := newTranscoder(t)
	p := write(t, "x.bin", []byte("not a video at all"))
	if _, err := tc.Probe(context.Background(), p); err == nil {
		t.Fatal("expected error")
	}
}

func TestTranscodeProducesBskyMp4(t *testing.T) {
	tc := newTranscoder(t)
	ctx := context.Background()
	in := write(t, "in.mov", mediatest.Fixture(t, "mpeg4", "libmp3lame", "mov"))
	out := filepath.Join(t.TempDir(), "out.mp4")
	if err := tc.Transcode(ctx, in, out); err != nil {
		t.Fatal(err)
	}
	info, err := tc.Probe(ctx, out)
	if err != nil {
		t.Fatal(err)
	}
	if info.VideoCodec != "h264" || info.PixFmt != "yuv420p" || info.AudioCodec != "aac" {
		t.Fatalf("output info = %+v", info)
	}
	if NeedsTranscode("video/mp4", info) {
		t.Fatal("transcoded output still needs transcode")
	}
	if info.Width != 320 || info.Height != 240 {
		t.Fatalf("dimensions = %dx%d", info.Width, info.Height)
	}
}

func TestTranscodeKeepsSilentVideoSilent(t *testing.T) {
	tc := newTranscoder(t)
	ctx := context.Background()
	in := write(t, "in.avi", mediatest.Fixture(t, "mpeg4", "", "avi"))
	out := filepath.Join(t.TempDir(), "out.mp4")
	if err := tc.Transcode(ctx, in, out); err != nil {
		t.Fatal(err)
	}
	info, err := tc.Probe(ctx, out)
	if err != nil {
		t.Fatal(err)
	}
	if info.AudioCodec != "" {
		t.Fatalf("audio stream appeared: %+v", info)
	}
}

func TestTranscodeFailsOnGarbage(t *testing.T) {
	tc := newTranscoder(t)
	in := write(t, "in.webm", []byte("garbage"))
	if err := tc.Transcode(context.Background(), in, filepath.Join(t.TempDir(), "o.mp4")); err == nil {
		t.Fatal("expected error")
	}
}
