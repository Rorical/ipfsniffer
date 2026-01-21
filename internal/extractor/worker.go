package extractor

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Rorical/IPFSniffer/internal/codec"
	"github.com/Rorical/IPFSniffer/internal/logging"
	internalnats "github.com/Rorical/IPFSniffer/internal/nats"
	"github.com/Rorical/IPFSniffer/internal/tika"
	ipfsnifferv1 "github.com/Rorical/IPFSniffer/proto"

	nats "github.com/nats-io/nats.go"
)

type Worker struct {
	NATS nats.JetStreamContext
	Tika *tika.Client

	Durable    string
	MaxDeliver int

	// Stream settings
	StreamMaxBytes int64

	// Extract limits
	TikaTimeout  time.Duration
	MaxTextBytes int64
}

func (w *Worker) Run(ctx context.Context) error {
	if w.NATS == nil {
		return fmt.Errorf("nats required")
	}
	if w.Tika == nil {
		return fmt.Errorf("tika client required")
	}
	if w.MaxTextBytes <= 0 {
		w.MaxTextBytes = 2_000_000
	}
	if w.TikaTimeout <= 0 {
		w.TikaTimeout = 60 * time.Second
	}

	durable := w.Durable
	if durable == "" {
		durable = "extractor"
	}

	if err := internalnats.EnsureConsumer(ctx, w.NATS, internalnats.SubjectFetchResult, durable, w.MaxDeliver); err != nil {
		return err
	}

	sub, err := w.NATS.PullSubscribe(internalnats.SubjectFetchResult, durable)
	if err != nil {
		return fmt.Errorf("pull subscribe: %w", err)
	}

	logger := logging.FromContext(ctx)
	logger.Info("extractor started", "subject", internalnats.SubjectFetchResult, "durable", durable)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		msgs, err := sub.Fetch(1, nats.MaxWait(2*time.Second))
		if err != nil {
			if err == nats.ErrTimeout {
				continue
			}
			return err
		}

		for _, msg := range msgs {
			if err := w.handle(ctx, msg); err != nil {
				logger.Error("extract handle", "err", err)
				continue
			}
			_ = msg.Ack()
		}
	}
}

func (w *Worker) handle(ctx context.Context, msg *nats.Msg) error {
	var fr ipfsnifferv1.FetchResult
	if err := codec.Unmarshal(msg.Data, &fr); err != nil {
		return err
	}

	d := fr.GetData()
	if d == nil {
		return nil
	}

	logger := logging.FromContext(ctx).With("root_cid", d.GetRootCid(), "path", d.GetPath())

	// Don't index failed/skipped fetch results; they are extremely common when
	// sniffing the public network and would flood the index with empty docs.
	if st := strings.ToLower(strings.TrimSpace(d.GetStatus())); st != "" && st != "ok" {
		return nil
	}

	// Extraction: inline if available; otherwise request stream and read chunks.
	contentIndexed := false
	text := ""
	textTruncated := false

	if d.GetNodeType() == "file" {
		var r io.Reader
		if d.GetContent().GetMode() == "inline" {
			raw, err := base64.StdEncoding.DecodeString(d.GetContent().GetInlineBase64())
			if err != nil {
				return err
			}
			r = bytesReader(raw)
		} else {
			// Request streaming from fetcher stream-server.
			maxBytes := d.GetSizeBytes()
			if maxBytes <= 0 {
				maxBytes = 10 * 1024 * 1024
			}
			if d.GetSizeBytes() > 0 {
				maxBytes = d.GetSizeBytes()
			}

			r2, err := w.streamReader(ctx, d.GetRootCid(), d.GetPath(), maxBytes)
			if err != nil {
				return err
			}
			r = r2
		}

		res, err := w.Tika.ExtractText(ctx, r, w.TikaTimeout, w.MaxTextBytes)
		if err != nil {
			// Tika 422 (unprocessable entity) or other extraction errors are common
			// when processing random network content. Log and continue without text.
			logger.Warn("tika extraction failed, continuing without text", "err", err)
			contentIndexed = false
			text = ""
			textTruncated = false
		} else {
			contentIndexed = true
			text = string(res.Text)
			textTruncated = res.Truncated
		}
	}

	out := &ipfsnifferv1.DocReady{
		V:     1,
		Id:    uuid.NewString(),
		Ts:    time.Now().UTC().Format(time.RFC3339Nano),
		Trace: fr.Trace,
		Data: &ipfsnifferv1.DocReadyData{
			RootCid:        d.GetRootCid(),
			Path:           d.GetPath(),
			NodeType:       d.GetNodeType(),
			Filename:       filenameFromPath(d.GetPath()),
			Ext:            d.GetExt(),
			Mime:           d.GetMime(),
			SizeBytes:      d.GetSizeBytes(),
			ContentIndexed: contentIndexed,
			Text:           text,
			TextTruncated:  textTruncated,
			NamesText:      filenameFromPath(d.GetPath()),
			Sources:        nil,
			ObservedAt:     "",
			ProcessedAt:    time.Now().UTC().Format(time.RFC3339Nano),
		},
	}

	b, err := codec.Marshal(out)
	if err != nil {
		return err
	}
	if _, err := internalnats.Publish(ctx, w.NATS, internalnats.SubjectDocReady, b); err != nil {
		_, _ = internalnats.PublishDLQ(ctx, w.NATS, internalnats.SubjectDocReady, b)
		return err
	}

	return nil
}

func (w *Worker) streamReader(ctx context.Context, rootCID string, p string, maxBytes int64) (io.Reader, error) {
	if w.NATS == nil {
		return nil, fmt.Errorf("nats required")
	}
	if maxBytes <= 0 {
		maxBytes = w.StreamMaxBytes
	}
	if maxBytes <= 0 {
		maxBytes = 10 * 1024 * 1024
	}

	streamID := uuid.NewString()
	chunkSubject := internalnats.StreamChunkSubject(streamID)

	sub, err := w.NATS.SubscribeSync(chunkSubject)
	if err != nil {
		return nil, fmt.Errorf("subscribe chunks: %w", err)
	}
	_ = sub.AutoUnsubscribe(100000)

	get := &ipfsnifferv1.StreamGet{
		V:    1,
		Id:   uuid.NewString(),
		Ts:   time.Now().UTC().Format(time.RFC3339Nano),
		Data: &ipfsnifferv1.StreamGetData{RootCid: rootCID, Path: p, MaxBytes: maxBytes},
	}
	b, err := codec.Marshal(get)
	if err != nil {
		return nil, err
	}
	if _, err := internalnats.Publish(ctx, w.NATS, internalnats.SubjectStreamGet, b); err != nil {
		return nil, err
	}

	return &chunkReader{ctx: ctx, sub: sub}, nil
}

type chunkReader struct {
	ctx context.Context
	sub *nats.Subscription
	buf []byte
	off int
	eof bool
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if r.eof {
		return 0, io.EOF
	}

	for {
		if r.off < len(r.buf) {
			n := copy(p, r.buf[r.off:])
			r.off += n
			return n, nil
		}

		msg, err := r.sub.NextMsgWithContext(r.ctx)
		if err != nil {
			return 0, err
		}
		var ch ipfsnifferv1.StreamChunk
		if err := codec.Unmarshal(msg.Data, &ch); err != nil {
			return 0, err
		}
		cd := ch.GetData()
		if cd.GetError() != "" {
			r.eof = true
			return 0, fmt.Errorf("stream error: %s", cd.GetError())
		}
		if cd.GetEof() {
			r.eof = true
			return 0, io.EOF
		}
		r.buf = cd.GetData()
		r.off = 0
	}
}

type byteReader struct {
	b []byte
	i int
}

func bytesReader(b []byte) *byteReader { return &byteReader{b: b} }

func (r *byteReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}

func filenameFromPath(p string) string {
	// cheap: take final segment
	for len(p) > 0 && p[len(p)-1] == '/' {
		p = p[:len(p)-1]
	}
	idx := strings.LastIndex(p, "/")
	if idx == -1 {
		return p
	}
	return p[idx+1:]
}
