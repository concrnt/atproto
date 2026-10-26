// Package media wraps the ffmpeg/ffprobe command line tools for the blob
// ingest path. Bluesky's app.bsky.embed.video takes exactly one shape of blob
// (an mp4 the appview's video CDN can repackage: H.264 yuv420p video, AAC or
// no audio), and the official app gets there by transcoding server side. The
// bridge writes records straight to its own PDS, so it has to do the same
// itself; anything that is not already in that shape goes through ffmpeg.
// Images are re-encoded too (see image.go): size-capped JPEG/PNG with EXIF
// stripped and orientation applied, the way the official app uploads them.
//
// Both tools are optional at runtime. When they are missing the bridge keeps
// its old behaviour: only video/mp4 and a few image types are accepted,
// unchecked apart from the size cap.
package media

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// Transcoder runs ffmpeg/ffprobe. A nil *Transcoder means transcoding is
// disabled; callers check for nil before use.
type Transcoder struct {
	ffmpeg  string
	ffprobe string
	timeout time.Duration
	alphaState
}

// New resolves the two binaries (names are looked up on PATH; absolute paths
// are used as given). timeout bounds one ffmpeg run; <= 0 means 10 minutes.
func New(ffmpegPath, ffprobePath string, timeout time.Duration) (*Transcoder, error) {
	if ffmpegPath == "" {
		ffmpegPath = "ffmpeg"
	}
	if ffprobePath == "" {
		ffprobePath = "ffprobe"
	}
	ffmpeg, err := exec.LookPath(ffmpegPath)
	if err != nil {
		return nil, fmt.Errorf("ffmpeg not found: %w", err)
	}
	ffprobe, err := exec.LookPath(ffprobePath)
	if err != nil {
		return nil, fmt.Errorf("ffprobe not found: %w", err)
	}
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	return &Transcoder{ffmpeg: ffmpeg, ffprobe: ffprobe, timeout: timeout}, nil
}

// Version returns the first line of `ffmpeg -version`, for startup logs.
func (t *Transcoder) Version(ctx context.Context) string {
	out, err := exec.CommandContext(ctx, t.ffmpeg, "-version").Output()
	if err != nil {
		return "unknown"
	}
	line, _, _ := strings.Cut(string(out), "\n")
	return strings.TrimSpace(line)
}

// Info is what the ingest path needs to know about a video file: the first
// video stream's codec and display dimensions (rotation applied), and the
// first audio stream's codec ("" when there is no audio).
type Info struct {
	Width      int
	Height     int
	VideoCodec string
	PixFmt     string
	AudioCodec string
	Duration   time.Duration
}

// ErrNoVideoStream is returned by Probe for files without a video stream.
var ErrNoVideoStream = errors.New("no video stream")

type probeStream struct {
	CodecType     string `json:"codec_type"`
	CodecName     string `json:"codec_name"`
	PixFmt        string `json:"pix_fmt"`
	Width         int    `json:"width"`
	Height        int    `json:"height"`
	NbReadPackets string `json:"nb_read_packets"`
	Tags          struct {
		Rotate string `json:"rotate"`
	} `json:"tags"`
	SideDataList []struct {
		Rotation json.Number `json:"rotation"`
	} `json:"side_data_list"`
}

// rotation returns the stream's display rotation in degrees from the rotate
// tag or display-matrix side data (side data wins), 0 when absent.
func (s probeStream) rotation() float64 {
	rot := 0.0
	if s.Tags.Rotate != "" {
		rot, _ = strconv.ParseFloat(s.Tags.Rotate, 64)
	}
	for _, sd := range s.SideDataList {
		if sd.Rotation != "" {
			if r, err := sd.Rotation.Float64(); err == nil {
				rot = r
			}
		}
	}
	return rot
}

type probeOutput struct {
	Streams []probeStream `json:"streams"`
	Format  struct {
		Duration string `json:"duration"`
	} `json:"format"`
}

// runProbe runs ffprobe on path with -show_streams -show_format plus extra
// flags and parses the JSON.
func (t *Transcoder) runProbe(ctx context.Context, path string, extra ...string) (*probeOutput, error) {
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	args := []string{"-v", "error", "-print_format", "json", "-show_streams", "-show_format"}
	args = append(args, extra...)
	args = append(args, path)
	cmd := exec.CommandContext(ctx, t.ffprobe, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("ffprobe failed: %w: %s", err, tail(stderr.String()))
	}
	var po probeOutput
	if err := json.Unmarshal(out, &po); err != nil {
		return nil, fmt.Errorf("ffprobe output unreadable: %w", err)
	}
	return &po, nil
}

// Probe inspects path with ffprobe.
func (t *Transcoder) Probe(ctx context.Context, path string) (*Info, error) {
	po, err := t.runProbe(ctx, path)
	if err != nil {
		return nil, err
	}

	info := &Info{}
	if secs, err := strconv.ParseFloat(po.Format.Duration, 64); err == nil {
		info.Duration = time.Duration(secs * float64(time.Second))
	}
	haveVideo := false
	for _, s := range po.Streams {
		switch s.CodecType {
		case "video":
			if haveVideo {
				continue
			}
			haveVideo = true
			info.VideoCodec = s.CodecName
			info.PixFmt = s.PixFmt
			info.Width, info.Height = s.Width, s.Height
			if r := int(s.rotation()) % 180; r == 90 || r == -90 {
				info.Width, info.Height = info.Height, info.Width
			}
		case "audio":
			if info.AudioCodec == "" {
				info.AudioCodec = s.CodecName
			}
		}
	}
	if !haveVideo {
		return nil, ErrNoVideoStream
	}
	return info, nil
}

// NeedsTranscode reports whether a file with the given media type and probe
// result has to be re-encoded to be a valid app.bsky.embed.video blob.
func NeedsTranscode(mime string, info *Info) bool {
	if mime != "video/mp4" {
		return true
	}
	if info.VideoCodec != "h264" || info.PixFmt != "yuv420p" {
		return true
	}
	if info.AudioCodec != "" && info.AudioCodec != "aac" {
		return true
	}
	return false
}

// Transcode re-encodes in to an H.264/AAC mp4 at out. The long side is capped
// at 1920px (the appview CDN downscales anyway and it keeps the 100MB blob
// cap reachable); audio is dropped only if the source has none.
func (t *Transcoder) Transcode(ctx context.Context, in, out string) error {
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, t.ffmpeg,
		"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-i", in,
		"-map", "0:v:0", "-map", "0:a:0?", "-sn", "-dn",
		"-c:v", "libx264", "-preset", "veryfast", "-profile:v", "high", "-pix_fmt", "yuv420p",
		"-vf", "scale='min(1920,iw)':-2",
		"-c:a", "aac", "-b:a", "128k", "-ac", "2",
		"-movflags", "+faststart",
		"-f", "mp4", out,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("ffmpeg timed out after %s", t.timeout)
		}
		return fmt.Errorf("ffmpeg failed: %w: %s", err, tail(stderr.String()))
	}
	return nil
}

// tail keeps the last few lines of ffmpeg's stderr for error messages.
func tail(s string) string {
	s = strings.TrimSpace(s)
	lines := strings.Split(s, "\n")
	if len(lines) > 5 {
		lines = lines[len(lines)-5:]
	}
	return strings.Join(lines, " | ")
}
