package fetcher

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"

	"github.com/Rorical/IPFSniffer/internal/codec"
	ipfs "github.com/Rorical/IPFSniffer/internal/kubo"
	"github.com/Rorical/IPFSniffer/internal/logging"
	internalnats "github.com/Rorical/IPFSniffer/internal/nats"
	ipfsnifferv1 "github.com/Rorical/IPFSniffer/proto"

	unixfile "github.com/ipfs/boxo/ipld/unixfs/file"
	boxopath "github.com/ipfs/boxo/path"
	nats "github.com/nats-io/nats.go"
)

const defaultChunkSize = 32 * 1024

type StreamServer struct {
	IPFS *ipfs.Node
	NATS nats.JetStreamContext

	ChunkSize int
}

func (s *StreamServer) Run(ctx context.Context) error {
	if s.IPFS == nil || s.IPFS.API == nil {
		return fmt.Errorf("ipfs node required")
	}
	if s.NATS == nil {
		return fmt.Errorf("nats jetstream required")
	}
	if s.ChunkSize <= 0 {
		s.ChunkSize = defaultChunkSize
	}

	if err := internalnats.EnsureConsumer(ctx, s.NATS, internalnats.SubjectStreamGet, "stream-server", internalnats.DefaultMaxDeliver); err != nil {
		return err
	}

	sub, err := s.NATS.PullSubscribe(internalnats.SubjectStreamGet, "stream-server")
	if err != nil {
		return err
	}

	logger := logging.FromContext(ctx)
	logger.Info("stream server started", "subject", internalnats.SubjectStreamGet)

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
			if err := s.handle(ctx, msg); err != nil {
				logger.Error("stream handle", "err", err)
				continue
			}
			_ = msg.Ack()
		}
	}
}

func (s *StreamServer) handle(ctx context.Context, msg *nats.Msg) error {
	var req ipfsnifferv1.StreamGet
	if err := codec.Unmarshal(msg.Data, &req); err != nil {
		return err
	}

	streamID := uuid.NewString()
	chunkSubject := internalnats.StreamChunkSubject(streamID)

	p := req.GetData().GetPath()
	maxBytes := req.GetData().GetMaxBytes()
	if maxBytes <= 0 {
		maxBytes = 1
	}

	ipfsPath, err := boxopath.NewPath(p)
	if err != nil {
		return s.sendErr(ctx, req.Trace, chunkSubject, streamID, err)
	}

	// Wrap with unixfile so we can read bytes.
	ipldNode, err := s.IPFS.API.ResolveNode(ctx, ipfsPath)
	if err != nil {
		return s.sendErr(ctx, req.Trace, chunkSubject, streamID, err)
	}

	node, err := unixfile.NewUnixfsFile(ctx, s.IPFS.Raw.DAG, ipldNode)
	if err != nil {
		return s.sendErr(ctx, req.Trace, chunkSubject, streamID, err)
	}
	defer node.Close()

	f, ok := node.(interface{ Read([]byte) (int, error) })
	if !ok {
		return s.sendErr(ctx, req.Trace, chunkSubject, streamID, fmt.Errorf("not readable"))
	}

	var sent int64
	seq := int64(0)
	buf := make([]byte, s.ChunkSize)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if sent >= maxBytes {
			return s.sendEOF(ctx, req.Trace, chunkSubject, streamID, seq)
		}

		want := len(buf)
		remaining := maxBytes - sent
		if remaining < int64(want) {
			want = int(remaining)
		}

		n, rerr := f.Read(buf[:want])
		if n > 0 {
			seq++
			sent += int64(n)
			chunk := &ipfsnifferv1.StreamChunk{
				V:     1,
				Id:    uuid.NewString(),
				Ts:    time.Now().UTC().Format(time.RFC3339Nano),
				Trace: req.Trace,
				Data:  &ipfsnifferv1.StreamChunkData{StreamId: streamID, Seq: seq, Data: buf[:n], Eof: false, Error: ""},
			}
			b, err := codec.Marshal(chunk)
			if err != nil {
				return err
			}
			if _, err := internalnats.Publish(ctx, s.NATS, chunkSubject, b); err != nil {
				return err
			}
		}

		if rerr != nil {
			if rerr == io.EOF {
				return s.sendEOF(ctx, req.Trace, chunkSubject, streamID, seq)
			}
			return s.sendErr(ctx, req.Trace, chunkSubject, streamID, rerr)
		}
	}
}

func (s *StreamServer) sendEOF(ctx context.Context, trace *ipfsnifferv1.TraceContext, subject, streamID string, seq int64) error {
	chunk := &ipfsnifferv1.StreamChunk{
		V:     1,
		Id:    uuid.NewString(),
		Ts:    time.Now().UTC().Format(time.RFC3339Nano),
		Trace: trace,
		Data:  &ipfsnifferv1.StreamChunkData{StreamId: streamID, Seq: seq + 1, Data: nil, Eof: true, Error: ""},
	}
	b, err := codec.Marshal(chunk)
	if err != nil {
		return err
	}
	_, err = internalnats.Publish(ctx, s.NATS, subject, b)
	return err
}

func (s *StreamServer) sendErr(ctx context.Context, trace *ipfsnifferv1.TraceContext, subject, streamID string, err error) error {
	chunk := &ipfsnifferv1.StreamChunk{
		V:     1,
		Id:    uuid.NewString(),
		Ts:    time.Now().UTC().Format(time.RFC3339Nano),
		Trace: trace,
		Data:  &ipfsnifferv1.StreamChunkData{StreamId: streamID, Seq: 1, Data: nil, Eof: true, Error: err.Error()},
	}
	b, mErr := codec.Marshal(chunk)
	if mErr != nil {
		return mErr
	}
	_, pErr := internalnats.Publish(ctx, s.NATS, subject, b)
	if pErr != nil {
		return pErr
	}
	return nil
}

// Base64 helper (kept for potential future content-ref use).
func encodeB64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
