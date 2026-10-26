package media

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

// Images referenced by concrnt posts are re-encoded before they become
// Bluesky blobs, for three reasons: app.bsky.embed.images caps a blob at
// 1,000,000 bytes (2,000,000 in the current lexicon) and the official client
// silently hides any post whose image is over the cap; camera files carry
// EXIF (GPS, device, timestamps) that must not be published; and ffmpeg can
// decode formats (HEIC/HEIF, AVIF, TIFF, BMP ...) that no Bluesky client
// renders. The output is a JPEG, or a PNG when the source has an alpha
// channel, scaled to fit MaxSide and compressed down a fixed ladder until it
// fits MaxBytes. Animated GIFs under the cap pass through untouched; over it,
// their first frame goes down the JPEG ladder.
//
// Every encode is deterministic (single thread, bitexact, explicit scale
// flags, no metadata) so that re-running the pipeline on the same source
// yields byte-identical output; blobs rely on that to restore lost files.

// ImageInfo describes the primary image stream of a still or animated image.
type ImageInfo struct {
	// Width/Height are display dimensions, i.e. with Orientation applied.
	Width, Height int
	// Codec is ffprobe's codec_name: mjpeg, png, webp, gif, hevc (HEIC),
	// av1 (AVIF), tiff, bmp, ...
	Codec  string
	PixFmt string
	// HasAlpha is true when the decoded pixel format carries an alpha plane.
	// GIFs always decode to bgra, so they count as alpha-capable.
	HasAlpha bool
	// Frames is the number of demuxed packets; >1 means animated.
	Frames int
	// Orientation is the EXIF orientation (1..8, 1 = upright) that has to be
	// applied to the stored pixels to get the display image.
	Orientation int
	// StreamIndex is the video stream to map (0:v:N). HEIF files expose
	// thumbnails as extra streams; the largest one is the primary image.
	StreamIndex int
}

// ImageOptions bounds the re-encoded output.
type ImageOptions struct {
	// MaxBytes is the output size cap (defaults to 1,000,000).
	MaxBytes int64
	// MaxSide caps the long side in pixels (defaults to 2000).
	MaxSide int
}

const (
	DefaultImageMaxBytes = 1_000_000
	DefaultImageMaxSide  = 2000
)

func (o ImageOptions) withDefaults() ImageOptions {
	if o.MaxBytes <= 0 {
		o.MaxBytes = DefaultImageMaxBytes
	}
	if o.MaxSide <= 0 {
		o.MaxSide = DefaultImageMaxSide
	}
	return o
}

// ImageResult is PrepareImage's output. Path equals the input path when the
// source was passed through unchanged (small animated GIF).
type ImageResult struct {
	Path          string
	Mime          string
	Width, Height int
	Size          int64
	// Attempts counts ffmpeg encodes that were run (0 on passthrough).
	Attempts int
}

// ErrImageTooLarge is returned (wrapped) when no rung of the ladder gets the
// image under MaxBytes.
var ErrImageTooLarge = errors.New("image cannot be compressed under the limit")

// ErrNoImageStream is returned by ProbeImage for files without an image.
var ErrNoImageStream = errors.New("no image stream")

// ProbeImage inspects an image file with ffprobe.
func (t *Transcoder) ProbeImage(ctx context.Context, path string) (*ImageInfo, error) {
	po, err := t.runProbe(ctx, path, "-count_packets")
	if err != nil {
		return nil, err
	}
	best := -1
	vidx := 0
	var info ImageInfo
	for _, s := range po.Streams {
		if s.CodecType != "video" {
			continue
		}
		if area := s.Width * s.Height; area > best {
			best = area
			info = ImageInfo{
				Width:       s.Width,
				Height:      s.Height,
				Codec:       s.CodecName,
				PixFmt:      s.PixFmt,
				StreamIndex: vidx,
				Orientation: rotationToOrientation(s.rotation()),
			}
			if n, err := strconv.Atoi(s.NbReadPackets); err == nil {
				info.Frames = n
			}
		}
		vidx++
	}
	if best < 0 {
		return nil, ErrNoImageStream
	}
	if info.Width <= 0 || info.Height <= 0 {
		return nil, fmt.Errorf("image has no dimensions (%s)", info.Codec)
	}
	if info.Orientation == 1 {
		// ffmpeg >= 8.1 exports EXIF/HEIF orientation as a display matrix on
		// the decoded frame rather than the stream; decode one frame to read
		// it. Older builds export nothing for JPEG, so the EXIF tag is parsed
		// directly as a last resort.
		if rot, err := t.firstFrameRotation(ctx, path, info.StreamIndex); err == nil {
			info.Orientation = rotationToOrientation(rot)
		}
	}
	if info.Orientation == 1 && info.Codec == "mjpeg" {
		if o, err := JPEGOrientation(path); err == nil && o > 1 {
			info.Orientation = o
		}
	}
	if info.Orientation >= 5 {
		info.Width, info.Height = info.Height, info.Width
	}
	info.HasAlpha = t.pixFmtHasAlpha(ctx, info.PixFmt)
	return &info, nil
}

// firstFrameRotation decodes the first frame of video stream idx and returns
// the rotation of its display-matrix side data (0 when there is none).
func (t *Transcoder) firstFrameRotation(ctx context.Context, path string, idx int) (float64, error) {
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, t.ffprobe,
		"-v", "error", "-print_format", "json",
		"-select_streams", fmt.Sprintf("v:%d", idx),
		"-show_frames", "-read_intervals", "%+#1",
		"-show_entries", "frame=side_data_list",
		path,
	).Output()
	if err != nil {
		return 0, err
	}
	var po struct {
		Frames []struct {
			SideDataList []struct {
				Rotation json.Number `json:"rotation"`
			} `json:"side_data_list"`
		} `json:"frames"`
	}
	if err := json.Unmarshal(out, &po); err != nil || len(po.Frames) == 0 {
		return 0, err
	}
	for _, sd := range po.Frames[0].SideDataList {
		if sd.Rotation != "" {
			if r, err := sd.Rotation.Float64(); err == nil {
				return r, nil
			}
		}
	}
	return 0, nil
}

// rotationToOrientation maps ffprobe's display-matrix rotation (degrees,
// counter-clockwise positive, as ffmpeg's autorotate reads it) to the EXIF
// orientation that produces the same display image.
func rotationToOrientation(rot float64) int {
	r := int(rot) % 360
	if r < 0 {
		r += 360
	}
	switch r {
	case 270: // rotation -90: content must turn 90° clockwise
		return 6
	case 90:
		return 8
	case 180:
		return 3
	}
	return 1
}

// orientationFilters returns the ffmpeg filter chain prefix that turns stored
// pixels into the display image for an EXIF orientation.
func orientationFilters(o int) string {
	switch o {
	case 2:
		return "hflip,"
	case 3:
		return "hflip,vflip,"
	case 4:
		return "vflip,"
	case 5:
		return "transpose=0,"
	case 6:
		return "transpose=1,"
	case 7:
		return "transpose=3,"
	case 8:
		return "transpose=2,"
	}
	return ""
}

// fitDims scales w×h down so the long side is at most maxSide, keeping the
// aspect ratio. It never upscales and never returns 0.
func fitDims(w, h, maxSide int) (int, int) {
	if max(w, h) <= maxSide {
		return w, h
	}
	if w >= h {
		return maxSide, max(1, (h*maxSide+w/2)/w)
	}
	return max(1, (w*maxSide+h/2)/h), maxSide
}

// pixFmtHasAlpha asks ffprobe once for the pixel formats flagged as carrying
// alpha and memoizes the set. pal8 (palette) is treated as alpha-capable
// since the palette may hold transparent entries; the cost of a false
// positive is one extra PNG encode before the JPEG fallback.
func (t *Transcoder) pixFmtHasAlpha(ctx context.Context, pixFmt string) bool {
	t.alphaOnce.Do(func() {
		t.alphaFmts = map[string]bool{"pal8": true}
		out, err := exec.CommandContext(ctx, t.ffprobe, "-v", "error", "-print_format", "json", "-show_pixel_formats").Output()
		if err != nil {
			// Fall back to a name heuristic covering the common formats.
			for _, f := range []string{"rgba", "bgra", "argb", "abgr", "ya8", "ya16le", "ya16be",
				"yuva420p", "yuva422p", "yuva444p", "gbrap", "gbrap16le", "gbrap16be", "rgba64le", "rgba64be", "bgra64le", "bgra64be"} {
				t.alphaFmts[f] = true
			}
			return
		}
		var po struct {
			PixelFormats []struct {
				Name  string `json:"name"`
				Flags struct {
					Alpha int `json:"alpha"`
				} `json:"flags"`
			} `json:"pixel_formats"`
		}
		if json.Unmarshal(out, &po) == nil {
			for _, f := range po.PixelFormats {
				if f.Flags.Alpha == 1 {
					t.alphaFmts[f.Name] = true
				}
			}
		}
	})
	return t.alphaFmts[pixFmt]
}

// imagePlan carries the per-source parameters shared by every ladder rung.
type imagePlan struct {
	in          string
	streamIndex int
	orientation int
	// flatten composites the (possibly transparent) source onto white; used
	// for alpha sources that fall back to JPEG and for GIF first frames.
	flatten bool
}

// jpegQualities is the ladder's per-dimension quality sequence (ffmpeg -q:v,
// 2 = best, 31 = worst); finalQualities is the last-resort sweep at the
// smallest dimension.
var (
	jpegQualities  = []int{3, 5, 8, 12}
	finalQualities = []int{18, 25, 31}
	ladderSides    = []int{1600, 1200, 800}
)

// PrepareImage re-encodes the image at in (declared media type mime) into
// outDir and returns the result. Small animated GIFs are passed through: the
// returned Path is in itself. On failure to fit MaxBytes it returns an error
// wrapping ErrImageTooLarge; any partial output is removed.
func (t *Transcoder) PrepareImage(ctx context.Context, in, mime, outDir string, opts ImageOptions) (*ImageResult, error) {
	opts = opts.withDefaults()
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()

	info, err := t.ProbeImage(ctx, in)
	if err != nil {
		return nil, err
	}
	plan := imagePlan{in: in, streamIndex: info.StreamIndex, orientation: info.Orientation}
	if mime == "image/gif" && info.Frames > 1 {
		st, err := os.Stat(in)
		if err != nil {
			return nil, err
		}
		if st.Size() <= opts.MaxBytes {
			return &ImageResult{Path: in, Mime: "image/gif", Width: info.Width, Height: info.Height, Size: st.Size()}, nil
		}
		// Oversized animation: keep the first frame as a still.
		plan.flatten = true
	} else if info.HasAlpha {
		plan.flatten = true
	}

	first := min(max(info.Width, info.Height), opts.MaxSide)
	sides := []int{first}
	for _, s := range ladderSides {
		if s < first {
			sides = append(sides, s)
		}
	}

	res := &ImageResult{}
	var smallest int64 = -1
	try := func(encode func(out string) error, ext, outMime string, w, h int) (bool, error) {
		f, err := os.CreateTemp(outDir, ".image-*"+ext)
		if err != nil {
			return false, err
		}
		out := f.Name()
		f.Close()
		res.Attempts++
		if err := encode(out); err != nil {
			os.Remove(out)
			return false, err
		}
		st, err := os.Stat(out)
		if err != nil {
			os.Remove(out)
			return false, err
		}
		if st.Size() <= opts.MaxBytes {
			res.Path, res.Mime, res.Width, res.Height, res.Size = out, outMime, w, h, st.Size()
			return true, nil
		}
		if smallest < 0 || st.Size() < smallest {
			smallest = st.Size()
		}
		os.Remove(out)
		return false, nil
	}

	// Alpha sources: PNG first, one attempt per dimension.
	if info.HasAlpha && !(mime == "image/gif" && info.Frames > 1) {
		for _, side := range sides {
			w, h := fitDims(info.Width, info.Height, side)
			ok, err := try(func(out string) error { return t.encodePNG(ctx, plan, w, h, out) }, ".png", "image/png", w, h)
			if err != nil {
				return nil, err
			}
			if ok {
				return res, nil
			}
		}
	}
	for _, side := range sides {
		w, h := fitDims(info.Width, info.Height, side)
		for _, q := range jpegQualities {
			ok, err := try(func(out string) error { return t.encodeJPEG(ctx, plan, w, h, q, out) }, ".jpg", "image/jpeg", w, h)
			if err != nil {
				return nil, err
			}
			if ok {
				return res, nil
			}
		}
	}
	w, h := fitDims(info.Width, info.Height, sides[len(sides)-1])
	for _, q := range finalQualities {
		ok, err := try(func(out string) error { return t.encodeJPEG(ctx, plan, w, h, q, out) }, ".jpg", "image/jpeg", w, h)
		if err != nil {
			return nil, err
		}
		if ok {
			return res, nil
		}
	}
	return nil, fmt.Errorf("%w: best attempt %d bytes after %d encodes (limit %d)", ErrImageTooLarge, smallest, res.Attempts, opts.MaxBytes)
}

const scaleFlags = "flags=lanczos+accurate_rnd+full_chroma_int"

// baseArgs are the deterministic, metadata-stripping flags every image encode
// starts with. Autorotate is off: orientation is applied explicitly from
// the probe so behaviour does not depend on which ffmpeg version exports
// which side data.
func (t *Transcoder) baseArgs(plan imagePlan) []string {
	return []string{
		"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-threads", "1", "-filter_threads", "1", "-filter_complex_threads", "1",
		"-noautorotate",
		"-i", plan.in,
	}
}

var outputArgs = []string{
	"-frames:v", "1", "-an", "-sn", "-dn",
	"-map_metadata", "-1", "-map_chapters", "-1",
	"-fflags", "+bitexact", "-flags", "+bitexact",
}

// toYUV converts to 4:2:0 for the JPEG encoder. The RGB→YUV conversion must
// be told to produce full range: swscale defaults to limited range (white =
// 235), and the encoder tags the file as full range, which would wash out
// every re-encoded PNG and flattened image.
const toYUV = "scale=" + scaleFlags + ":out_range=pc,format=yuv420p"

func (t *Transcoder) encodeJPEG(ctx context.Context, plan imagePlan, w, h, q int, out string) error {
	args := t.baseArgs(plan)
	scale := fmt.Sprintf("%sscale=%d:%d:%s", orientationFilters(plan.orientation), w, h, scaleFlags)
	if plan.flatten {
		args = append(args,
			"-f", "lavfi", "-i", fmt.Sprintf("color=c=white:s=%dx%d", w, h),
			"-filter_complex", fmt.Sprintf("[0:v:%d]%s,format=rgba[fg];[1:v][fg]overlay=format=auto:shortest=1,%s[out]", plan.streamIndex, scale, toYUV),
			"-map", "[out]",
		)
	} else {
		args = append(args,
			"-map", fmt.Sprintf("0:v:%d", plan.streamIndex),
			"-vf", scale+":out_range=pc,format=yuv420p",
		)
	}
	args = append(args, outputArgs...)
	args = append(args,
		"-c:v", "mjpeg", "-q:v", strconv.Itoa(q), "-huffman", "optimal",
		"-pix_fmt", "yuv420p", "-color_range", "pc",
		"-f", "image2", "-update", "1", out,
	)
	return t.runFFmpeg(ctx, args)
}

func (t *Transcoder) encodePNG(ctx context.Context, plan imagePlan, w, h int, out string) error {
	args := t.baseArgs(plan)
	args = append(args,
		"-map", fmt.Sprintf("0:v:%d", plan.streamIndex),
		"-vf", fmt.Sprintf("%sscale=%d:%d:%s,format=rgba", orientationFilters(plan.orientation), w, h, scaleFlags),
	)
	args = append(args, outputArgs...)
	args = append(args,
		"-c:v", "png", "-compression_level", "9", "-pred", "mixed",
		"-f", "image2", "-update", "1", out,
	)
	return t.runFFmpeg(ctx, args)
}

func (t *Transcoder) runFFmpeg(ctx context.Context, args []string) error {
	cmd := exec.CommandContext(ctx, t.ffmpeg, args...)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("ffmpeg timed out after %s", t.timeout)
		}
		return fmt.Errorf("ffmpeg failed: %w: %s", err, tail(stderr.String()))
	}
	return nil
}

// alphaState is embedded in Transcoder for the memoized alpha pixel-format set.
type alphaState struct {
	alphaOnce sync.Once
	alphaFmts map[string]bool
}
