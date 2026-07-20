package pds

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/events"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
)

// handleSubscribeRepos serves the com.atproto.sync.subscribeRepos firehose.
// Adapted from indigo's splitter WebSocket handler.
func (s *Server) handleSubscribeRepos(c echo.Context) error {
	var since *int64
	if sinceVal := c.QueryParam("cursor"); sinceVal != "" {
		sval, err := strconv.ParseInt(sinceVal, 10, 64)
		if err != nil {
			return xrpcError(c, 400, "InvalidRequest", "malformed cursor")
		}
		since = &sval
	}

	// The request context lives as long as the WebSocket does.
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()

	upgrader := websocket.Upgrader{
		ReadBufferSize:  10 << 10,
		WriteBufferSize: 10 << 10,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}
	conn, err := upgrader.Upgrade(c.Response(), c.Request(), c.Response().Header())
	if err != nil {
		return fmt.Errorf("upgrading websocket: %w", err)
	}
	defer conn.Close()

	lastWriteLk := sync.Mutex{}
	lastWrite := time.Now()

	// Ping the client if the stream has been quiet, so dead consumers get
	// torn down instead of lingering.
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				lastWriteLk.Lock()
				lw := lastWrite
				lastWriteLk.Unlock()
				if time.Since(lw) < 30*time.Second {
					continue
				}
				if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(5*time.Second)); err != nil {
					cancel()
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	conn.SetPingHandler(func(message string) error {
		err := conn.WriteControl(websocket.PongMessage, []byte(message), time.Now().Add(time.Second*60))
		if err == websocket.ErrCloseSent {
			return nil
		} else if e, ok := err.(net.Error); ok && e.Timeout() {
			return nil
		}
		return err
	})

	// Drain (and ignore) client messages; a read error ends the session.
	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				cancel()
				return
			}
		}
	}()

	ident := c.RealIP() + "-" + c.Request().UserAgent()
	evts, cleanup, err := s.repos.Events().Subscribe(ctx, ident, func(evt *events.XRPCStreamEvent) bool { return true }, since)
	if err != nil {
		return err
	}
	defer cleanup()

	slog.Info("new firehose consumer", "remote", c.RealIP(), "ua", c.Request().UserAgent(), "cursor", since)

	for {
		select {
		case evt, ok := <-evts:
			if !ok {
				return nil
			}
			wc, err := conn.NextWriter(websocket.BinaryMessage)
			if err != nil {
				return err
			}
			if evt.Preserialized != nil {
				_, err = wc.Write(evt.Preserialized)
			} else {
				err = evt.Serialize(wc)
			}
			if err != nil {
				return fmt.Errorf("failed to write event: %w", err)
			}
			if err := wc.Close(); err != nil {
				return nil
			}
			lastWriteLk.Lock()
			lastWrite = time.Now()
			lastWriteLk.Unlock()
		case <-ctx.Done():
			return nil
		}
	}
}
