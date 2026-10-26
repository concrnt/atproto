// Package mediatest generates small video and image fixtures with ffmpeg for
// tests. Tests that use it skip when ffmpeg is not installed.
package mediatest

import (
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

// AVIFFixture renders a w×h AVIF (AV1 in a HEIF container). It skips when
// the local ffmpeg has no avif muxer or libaom encoder (HEIF support arrived
// in FFmpeg 7.1). Decoding it exercises the same HEIF demuxer path HEIC uses.
func AVIFFixture(t testing.TB, w, h int) []byte {
	t.Helper()
	Require(t)
	out, err := exec.Command("ffmpeg", "-hide_banner", "-muxers").Output()
	if err != nil || !strings.Contains(string(out), " avif ") {
		t.Skip("ffmpeg without avif muxer")
	}
	p := filepath.Join(t.TempDir(), "fixture.avif")
	args := []string{"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-f", "lavfi", "-i", fmt.Sprintf("testsrc=size=%dx%d", w, h),
		"-frames:v", "1", "-c:v", "libaom-av1", "-cpu-used", "8", "-pix_fmt", "yuv420p",
		"-f", "avif", p}
	return render(t, p, args)
}

// RequireHEIF skips the test when the local ffmpeg cannot demux HEIF: it
// probes a generated AVIF, which uses the same container path as HEIC.
func RequireHEIF(t testing.TB) {
	t.Helper()
	avif := AVIFFixture(t, 16, 16)
	p := filepath.Join(t.TempDir(), "probe.avif")
	if err := os.WriteFile(p, avif, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := exec.Command("ffprobe", "-v", "error", "-show_streams", p).Run(); err != nil {
		t.Skip("ffmpeg without HEIF demuxer")
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
	return render(t, out, args)
}

func render(t testing.TB, out string, args []string) []byte {
	t.Helper()
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

// ImageFixture renders a single w×h test pattern frame with the given
// encoder (mjpeg, png, bmp, ...) and returns the file's bytes.
func ImageFixture(t testing.TB, codec string, w, h int) []byte {
	t.Helper()
	Require(t)
	out := filepath.Join(t.TempDir(), "fixture."+codec)
	args := []string{"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-f", "lavfi", "-i", fmt.Sprintf("testsrc=size=%dx%d", w, h),
		"-frames:v", "1", "-c:v", codec}
	if codec == "mjpeg" {
		args = append(args, "-pix_fmt", "yuvj420p")
	}
	args = append(args, "-f", "image2", "-update", "1", out)
	return render(t, out, args)
}

// SolidPNG renders a w×h PNG filled with one color (an ffmpeg color name
// or 0xRRGGBB).
func SolidPNG(t testing.TB, color string, w, h int) []byte {
	t.Helper()
	Require(t)
	out := filepath.Join(t.TempDir(), "solid.png")
	args := []string{"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-f", "lavfi", "-i", fmt.Sprintf("color=c=%s:s=%dx%d", color, w, h),
		"-frames:v", "1", "-c:v", "png", "-f", "image2", "-update", "1", out}
	return render(t, out, args)
}

// NoisyPNG renders w×h of RGB noise as a PNG: incompressible content that
// forces the image ladder to shrink and lower quality.
func NoisyPNG(t testing.TB, w, h int) []byte {
	t.Helper()
	Require(t)
	out := filepath.Join(t.TempDir(), "noise.png")
	args := []string{"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-f", "lavfi", "-i", fmt.Sprintf("nullsrc=size=%dx%d,format=rgb24,geq=r='random(1)*255':g='random(2)*255':b='random(3)*255'", w, h),
		"-frames:v", "1", "-c:v", "png", "-f", "image2", "-update", "1", out}
	return render(t, out, args)
}

// RGBAPNG renders a w×h test pattern whose left half is fully transparent.
func RGBAPNG(t testing.TB, w, h int) []byte {
	t.Helper()
	Require(t)
	out := filepath.Join(t.TempDir(), "alpha.png")
	args := []string{"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-f", "lavfi", "-i", fmt.Sprintf("testsrc=size=%dx%d", w, h),
		"-frames:v", "1",
		"-vf", "format=rgba,geq=r='r(X,Y)':g='g(X,Y)':b='b(X,Y)':a='if(lt(X,W/2),0,255)'",
		"-c:v", "png", "-f", "image2", "-update", "1", out}
	return render(t, out, args)
}

// NoisyRGBAPNG is RGBAPNG with noise in the opaque half, so that PNG cannot
// compress it under a small limit.
func NoisyRGBAPNG(t testing.TB, w, h int) []byte {
	t.Helper()
	Require(t)
	out := filepath.Join(t.TempDir(), "alpha-noise.png")
	args := []string{"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-f", "lavfi", "-i", fmt.Sprintf("nullsrc=size=%dx%d,format=rgba,geq=r='random(1)*255':g='random(2)*255':b='random(3)*255':a='if(lt(X,W/2),0,255)'", w, h),
		"-frames:v", "1", "-c:v", "png", "-f", "image2", "-update", "1", out}
	return render(t, out, args)
}

// AnimatedGIF renders a w×h GIF with the given number of frames.
func AnimatedGIF(t testing.TB, frames, w, h int) []byte {
	t.Helper()
	Require(t)
	out := filepath.Join(t.TempDir(), "anim.gif")
	args := []string{"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-f", "lavfi", "-i", fmt.Sprintf("testsrc=size=%dx%d:rate=%d:duration=1", w, h, frames),
		"-f", "gif", out}
	return render(t, out, args)
}

// WithEXIFOrientation returns jpeg with an APP1 Exif segment carrying the
// given Orientation (1..8) spliced in right after SOI. ffmpeg ignores the
// segment's content, so the result still decodes; byteOrder is "II" or "MM".
func WithEXIFOrientation(t testing.TB, jpeg []byte, orientation int, byteOrder string) []byte {
	t.Helper()
	if len(jpeg) < 2 || jpeg[0] != 0xFF || jpeg[1] != 0xD8 {
		t.Fatal("not a JPEG")
	}
	var bo binary.AppendByteOrder = binary.LittleEndian
	if byteOrder == "MM" {
		bo = binary.BigEndian
	} else if byteOrder != "II" {
		t.Fatalf("byte order %q", byteOrder)
	}
	tiff := make([]byte, 0, 8+2+12+4)
	tiff = append(tiff, byteOrder...)
	tiff = bo.AppendUint16(tiff, 42)
	tiff = bo.AppendUint32(tiff, 8) // IFD0 right after the header
	tiff = bo.AppendUint16(tiff, 1) // one entry
	tiff = bo.AppendUint16(tiff, 0x0112)
	tiff = bo.AppendUint16(tiff, 3) // SHORT
	tiff = bo.AppendUint32(tiff, 1)
	tiff = bo.AppendUint16(tiff, uint16(orientation))
	tiff = bo.AppendUint16(tiff, 0) // value padding
	tiff = bo.AppendUint32(tiff, 0) // next IFD: none
	payload := append([]byte("Exif\x00\x00"), tiff...)
	seg := []byte{0xFF, 0xE1}
	seg = binary.BigEndian.AppendUint16(seg, uint16(len(payload)+2))
	seg = append(seg, payload...)
	out := append([]byte{0xFF, 0xD8}, seg...)
	return append(out, jpeg[2:]...)
}
