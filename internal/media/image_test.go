package media

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/concrnt/atproto/internal/media/mediatest"
)

var (
	sampleFlag = flag.String("sample", "", "image file to run through PrepareImage (manual check)")
	outFlag    = flag.String("out", "", "where to copy the -sample result")
)

func TestJPEGOrientation(t *testing.T) {
	base := []byte{0xFF, 0xD8, 0xFF, 0xDB, 0x00, 0x04, 0x00, 0x00, 0xFF, 0xDA, 0x00, 0x02}
	if got := jpegOrientation(base); got != 0 {
		t.Fatalf("no EXIF: got %d", got)
	}
	if got := jpegOrientation([]byte("\x89PNG")); got != 0 {
		t.Fatalf("png: got %d", got)
	}
	for _, bo := range []string{"II", "MM"} {
		for o := 1; o <= 8; o++ {
			data := mediatest.WithEXIFOrientation(t, base, o, bo)
			if got := jpegOrientation(data); got != o {
				t.Fatalf("%s orientation %d: got %d", bo, o, got)
			}
		}
	}
	// Out-of-range value is ignored.
	if got := jpegOrientation(mediatest.WithEXIFOrientation(t, base, 9, "II")); got != 0 {
		t.Fatalf("orientation 9: got %d", got)
	}
	p := write(t, "o.jpg", mediatest.WithEXIFOrientation(t, base, 6, "MM"))
	got, err := JPEGOrientation(p)
	if err != nil || got != 6 {
		t.Fatalf("JPEGOrientation = %d, %v", got, err)
	}
}

func TestFitDims(t *testing.T) {
	cases := []struct{ w, h, side, ww, hh int }{
		{640, 480, 2000, 640, 480},
		{4000, 3000, 2000, 2000, 1500},
		{3000, 4000, 2000, 1500, 2000},
		{3000, 2000, 2000, 2000, 1333},
		{10000, 1, 800, 800, 1},
	}
	for _, c := range cases {
		if w, h := fitDims(c.w, c.h, c.side); w != c.ww || h != c.hh {
			t.Errorf("fitDims(%d,%d,%d) = %dx%d, want %dx%d", c.w, c.h, c.side, w, h, c.ww, c.hh)
		}
	}
}

func TestRotationToOrientation(t *testing.T) {
	for rot, want := range map[float64]int{0: 1, -90: 6, 270: 6, 90: 8, -270: 8, 180: 3, -180: 3, 45: 1} {
		if got := rotationToOrientation(rot); got != want {
			t.Errorf("rotation %v: got %d, want %d", rot, got, want)
		}
	}
}

func TestProbeImage(t *testing.T) {
	tc := newTranscoder(t)
	ctx := context.Background()

	jpg := mediatest.ImageFixture(t, "mjpeg", 400, 300)
	info, err := tc.ProbeImage(ctx, write(t, "a.jpg", jpg))
	if err != nil {
		t.Fatal(err)
	}
	if info.Codec != "mjpeg" || info.HasAlpha || info.Frames != 1 || info.Orientation != 1 || info.Width != 400 || info.Height != 300 {
		t.Fatalf("jpeg info = %+v", info)
	}

	info, err = tc.ProbeImage(ctx, write(t, "o6.jpg", mediatest.WithEXIFOrientation(t, jpg, 6, "II")))
	if err != nil {
		t.Fatal(err)
	}
	if info.Orientation != 6 || info.Width != 300 || info.Height != 400 {
		t.Fatalf("exif 6 info = %+v", info)
	}

	info, err = tc.ProbeImage(ctx, write(t, "a.png", mediatest.RGBAPNG(t, 64, 48)))
	if err != nil {
		t.Fatal(err)
	}
	if info.Codec != "png" || !info.HasAlpha || info.PixFmt != "rgba" {
		t.Fatalf("rgba info = %+v", info)
	}

	info, err = tc.ProbeImage(ctx, write(t, "a.gif", mediatest.AnimatedGIF(t, 5, 120, 90)))
	if err != nil {
		t.Fatal(err)
	}
	if info.Codec != "gif" || info.Frames != 5 || !info.HasAlpha {
		t.Fatalf("gif info = %+v", info)
	}

	if _, err := tc.ProbeImage(ctx, write(t, "x.bin", []byte("not an image"))); err == nil {
		t.Fatal("expected error for garbage")
	}
}

func prepare(t *testing.T, tc *Transcoder, name string, data []byte, mime string, opts ImageOptions) (*ImageResult, string) {
	t.Helper()
	dir := t.TempDir()
	in := filepath.Join(dir, name)
	if err := os.WriteFile(in, data, 0o644); err != nil {
		t.Fatal(err)
	}
	res, err := tc.PrepareImage(context.Background(), in, mime, dir, opts)
	if err != nil {
		t.Fatalf("PrepareImage: %v", err)
	}
	return res, dir
}

func assertNoMetadata(t *testing.T, data []byte) {
	t.Helper()
	if bytes.Contains(data, []byte("Exif\x00\x00")) {
		t.Fatal("output still carries an EXIF segment")
	}
	if bytes.Contains(data, []byte("Lavc")) {
		t.Fatal("output carries an encoder comment")
	}
}

// leftovers returns files in dir other than the input and the result.
func leftovers(t *testing.T, dir string, keep ...string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var extra []string
	for _, e := range entries {
		p := filepath.Join(dir, e.Name())
		kept := false
		for _, k := range keep {
			if k == p {
				kept = true
			}
		}
		if !kept {
			extra = append(extra, e.Name())
		}
	}
	return extra
}

func TestPrepareImageJPEGUnderCap(t *testing.T) {
	tc := newTranscoder(t)
	res, dir := prepare(t, tc, "in.jpg", mediatest.ImageFixture(t, "mjpeg", 640, 480), "image/jpeg", ImageOptions{})
	if res.Mime != "image/jpeg" || res.Width != 640 || res.Height != 480 || res.Attempts != 1 {
		t.Fatalf("result = %+v", res)
	}
	data, _ := os.ReadFile(res.Path)
	if int64(len(data)) != res.Size || res.Size > DefaultImageMaxBytes {
		t.Fatalf("size = %d (reported %d)", len(data), res.Size)
	}
	assertNoMetadata(t, data)
	info, err := tc.ProbeImage(context.Background(), res.Path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Codec != "mjpeg" || info.PixFmt != "yuvj420p" {
		t.Fatalf("output probe = %+v", info)
	}
	if extra := leftovers(t, dir, filepath.Join(dir, "in.jpg"), res.Path); len(extra) != 0 {
		t.Fatalf("leftovers: %v", extra)
	}
}

func TestPrepareImageDownscalesLongSide(t *testing.T) {
	tc := newTranscoder(t)
	res, _ := prepare(t, tc, "in.jpg", mediatest.ImageFixture(t, "mjpeg", 4000, 3000), "image/jpeg", ImageOptions{})
	if res.Width != 2000 || res.Height != 1500 {
		t.Fatalf("dims = %dx%d", res.Width, res.Height)
	}
	info, err := tc.ProbeImage(context.Background(), res.Path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Width != 2000 || info.Height != 1500 {
		t.Fatalf("output probe = %+v", info)
	}
	// Odd target heights (3000x2000 -> 2000x1333) must encode too.
	res, _ = prepare(t, tc, "odd.jpg", mediatest.ImageFixture(t, "mjpeg", 3000, 2000), "image/jpeg", ImageOptions{})
	if res.Width != 2000 || res.Height != 1333 {
		t.Fatalf("odd dims = %dx%d", res.Width, res.Height)
	}
}

func TestPrepareImageAppliesOrientation(t *testing.T) {
	tc := newTranscoder(t)
	src := mediatest.WithEXIFOrientation(t, mediatest.ImageFixture(t, "mjpeg", 400, 300), 6, "II")
	res, _ := prepare(t, tc, "in.jpg", src, "image/jpeg", ImageOptions{})
	if res.Width != 300 || res.Height != 400 {
		t.Fatalf("dims = %dx%d, want 300x400", res.Width, res.Height)
	}
	data, _ := os.ReadFile(res.Path)
	assertNoMetadata(t, data)
	info, err := tc.ProbeImage(context.Background(), res.Path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Width != 300 || info.Height != 400 || info.Orientation != 1 {
		t.Fatalf("output probe = %+v", info)
	}
}

func TestPrepareImageLadderDescendsDeterministically(t *testing.T) {
	tc := newTranscoder(t)
	src := mediatest.NoisyPNG(t, 2000, 1500)
	res, _ := prepare(t, tc, "noise.png", src, "image/png", ImageOptions{})
	if res.Mime != "image/jpeg" || res.Size > DefaultImageMaxBytes || res.Attempts < 2 {
		t.Fatalf("result = %+v", res)
	}
	if res.Width >= 2000 && res.Height >= 1500 {
		// Noise at 2000x1500 is ~1.3MB even at q=31 (see ladder design), so
		// the ladder must have stepped the dimensions down.
		t.Fatalf("dimensions did not shrink: %dx%d", res.Width, res.Height)
	}
	first, _ := os.ReadFile(res.Path)
	res2, _ := prepare(t, tc, "noise.png", src, "image/png", ImageOptions{})
	second, _ := os.ReadFile(res2.Path)
	if !bytes.Equal(first, second) {
		t.Fatal("re-encoding the same source produced different bytes")
	}
}

func TestPrepareImageTooSmallLimit(t *testing.T) {
	tc := newTranscoder(t)
	dir := t.TempDir()
	in := filepath.Join(dir, "in.png")
	if err := os.WriteFile(in, mediatest.NoisyPNG(t, 400, 300), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := tc.PrepareImage(context.Background(), in, "image/png", dir, ImageOptions{MaxBytes: 500})
	if !errors.Is(err, ErrImageTooLarge) {
		t.Fatalf("err = %v, want ErrImageTooLarge", err)
	}
	if extra := leftovers(t, dir, in); len(extra) != 0 {
		t.Fatalf("leftovers after failure: %v", extra)
	}
}

func TestPrepareImageKeepsAlphaAsPNG(t *testing.T) {
	tc := newTranscoder(t)
	res, _ := prepare(t, tc, "in.png", mediatest.RGBAPNG(t, 320, 240), "image/png", ImageOptions{})
	if res.Mime != "image/png" || res.Width != 320 || res.Height != 240 {
		t.Fatalf("result = %+v", res)
	}
	info, err := tc.ProbeImage(context.Background(), res.Path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Codec != "png" || !info.HasAlpha {
		t.Fatalf("output probe = %+v", info)
	}
	data, _ := os.ReadFile(res.Path)
	if bytes.Contains(data, []byte("tEXt")) {
		t.Fatal("png carries text metadata")
	}
}

// topLeftPixel decodes the first pixel of an image as RGB.
func topLeftPixel(t *testing.T, path string) [3]byte {
	t.Helper()
	out, err := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-i", path,
		"-vf", "crop=2:2:0:0", "-frames:v", "1", "-f", "rawvideo", "-pix_fmt", "rgb24", "-").Output()
	if err != nil || len(out) < 3 {
		t.Fatalf("pixel read: %v (%d bytes)", err, len(out))
	}
	return [3]byte{out[0], out[1], out[2]}
}

func TestPrepareImageKeepsFullRange(t *testing.T) {
	tc := newTranscoder(t)
	// An opaque RGB source converted to JPEG must stay full range: pure
	// white in, pure white out (not the limited-range 235).
	res, _ := prepare(t, tc, "white.png", mediatest.SolidPNG(t, "white", 64, 64), "image/png", ImageOptions{})
	if res.Mime != "image/jpeg" {
		t.Fatalf("result = %+v", res)
	}
	if px := topLeftPixel(t, res.Path); px[0] < 254 || px[1] < 254 || px[2] < 254 {
		t.Fatalf("white became %v", px)
	}
	res, _ = prepare(t, tc, "black.png", mediatest.SolidPNG(t, "black", 64, 64), "image/png", ImageOptions{})
	if px := topLeftPixel(t, res.Path); px[0] > 1 || px[1] > 1 || px[2] > 1 {
		t.Fatalf("black became %v", px)
	}
}

func TestPrepareImageAlphaFallsBackToFlattenedJPEG(t *testing.T) {
	tc := newTranscoder(t)
	// 1200x900 noise cannot be a PNG under 200KB; the JPEG ladder can.
	res, _ := prepare(t, tc, "in.png", mediatest.NoisyRGBAPNG(t, 1200, 900), "image/png", ImageOptions{MaxBytes: 200_000})
	if res.Mime != "image/jpeg" || res.Size > 200_000 {
		t.Fatalf("result = %+v", res)
	}
	if px := topLeftPixel(t, res.Path); px[0] < 250 || px[1] < 250 || px[2] < 250 {
		t.Fatalf("transparent area not flattened on white: %v", px)
	}
}

func TestPrepareImageAnimatedGIF(t *testing.T) {
	tc := newTranscoder(t)
	gif := mediatest.AnimatedGIF(t, 5, 120, 90)

	res, dir := prepare(t, tc, "in.gif", gif, "image/gif", ImageOptions{})
	if res.Path != filepath.Join(dir, "in.gif") || res.Mime != "image/gif" || res.Attempts != 0 || res.Size != int64(len(gif)) {
		t.Fatalf("small gif was not passed through: %+v", res)
	}
	if res.Width != 120 || res.Height != 90 {
		t.Fatalf("gif dims = %dx%d", res.Width, res.Height)
	}

	res, _ = prepare(t, tc, "big.gif", gif, "image/gif", ImageOptions{MaxBytes: int64(len(gif)) - 1})
	if res.Mime != "image/jpeg" {
		t.Fatalf("oversize gif = %+v", res)
	}
	info, err := tc.ProbeImage(context.Background(), res.Path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Frames != 1 || info.Width != 120 || info.Height != 90 {
		t.Fatalf("first frame probe = %+v", info)
	}
}

func TestPrepareImageAcceptsOtherFormats(t *testing.T) {
	tc := newTranscoder(t)
	res, _ := prepare(t, tc, "in.bmp", mediatest.ImageFixture(t, "bmp", 200, 100), "image/bmp", ImageOptions{})
	if res.Mime != "image/jpeg" || res.Width != 200 || res.Height != 100 {
		t.Fatalf("bmp result = %+v", res)
	}
}

func TestPrepareImageRejectsGarbage(t *testing.T) {
	tc := newTranscoder(t)
	dir := t.TempDir()
	in := filepath.Join(dir, "in.jpg")
	os.WriteFile(in, []byte("garbage"), 0o644)
	if _, err := tc.PrepareImage(context.Background(), in, "image/jpeg", dir, ImageOptions{}); err == nil {
		t.Fatal("expected error")
	}
	if extra := leftovers(t, dir, in); len(extra) != 0 {
		t.Fatalf("leftovers: %v", extra)
	}
}

// TestPrepareImageAVIF covers the HEIF container path (what HEIC uses) with a
// generated AVIF; it skips on ffmpeg builds without avif muxing.
func TestPrepareImageAVIF(t *testing.T) {
	tc := newTranscoder(t)
	res, _ := prepare(t, tc, "in.avif", mediatest.AVIFFixture(t, 320, 240), "image/avif", ImageOptions{})
	if res.Mime != "image/jpeg" || res.Width != 320 || res.Height != 240 {
		t.Fatalf("avif result = %+v", res)
	}
}

// TestPrepareImageHEIC runs only when testdata/sample.heic exists and the
// local ffmpeg demuxes HEIF (ffmpeg >= 7.1; the Docker image's static build).
func TestPrepareImageHEIC(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("testdata", "sample.heic"))
	if err != nil {
		t.Skip("no testdata/sample.heic")
	}
	mediatest.RequireHEIF(t)
	tc := newTranscoder(t)
	res, _ := prepare(t, tc, "in.heic", src, "image/heic", ImageOptions{})
	if res.Mime != "image/jpeg" || res.Size > DefaultImageMaxBytes || res.Width == 0 {
		t.Fatalf("heic result = %+v", res)
	}
}

// TestPrepareImageSampleFile is a manual check: go test ./internal/media
// -run SampleFile -v -args -sample /path/to/photo.jpg [-out /path/result].
func TestPrepareImageSampleFile(t *testing.T) {
	if *sampleFlag == "" {
		t.Skip("no -sample")
	}
	tc := newTranscoder(t)
	src, err := os.ReadFile(*sampleFlag)
	if err != nil {
		t.Fatal(err)
	}
	mime := "image/" + strings.TrimPrefix(strings.ToLower(filepath.Ext(*sampleFlag)), ".")
	if mime == "image/jpg" {
		mime = "image/jpeg"
	}
	info, err := tc.ProbeImage(context.Background(), *sampleFlag)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %d bytes, probe %+v", len(src), info)
	res, _ := prepare(t, tc, "sample"+filepath.Ext(*sampleFlag), src, mime, ImageOptions{})
	t.Logf("result: %+v", res)
	if *outFlag != "" {
		data, _ := os.ReadFile(res.Path)
		if err := os.WriteFile(*outFlag, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
