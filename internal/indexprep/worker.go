package indexprep

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/Rorical/IPFSniffer/internal/codec"
	internalnats "github.com/Rorical/IPFSniffer/internal/nats"
	ipfsnifferv1 "github.com/Rorical/IPFSniffer/proto"

	"google.golang.org/protobuf/types/known/structpb"

	nats "github.com/nats-io/nats.go"
)

type Worker struct {
	NATS       nats.JetStreamContext
	Durable    string
	MaxDeliver int

	IndexName string
}

func (w *Worker) Run(ctx context.Context) error {
	if w.NATS == nil {
		return fmt.Errorf("nats required")
	}
	if w.IndexName == "" {
		w.IndexName = "ipfsniffer-docs-v1"
	}
	if w.Durable == "" {
		w.Durable = "index-prep"
	}

	if err := internalnats.EnsureConsumer(ctx, w.NATS, internalnats.SubjectDocReady, w.Durable, w.MaxDeliver); err != nil {
		return err
	}

	sub, err := w.NATS.PullSubscribe(internalnats.SubjectDocReady, w.Durable)
	if err != nil {
		return err
	}

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
				continue
			}
			_ = msg.Ack()
		}
	}
}

func (w *Worker) handle(ctx context.Context, msg *nats.Msg) error {
	var in ipfsnifferv1.DocReady
	if err := codec.Unmarshal(msg.Data, &in); err != nil {
		return err
	}
	d := in.GetData()
	if d == nil {
		return nil
	}

	docID := deterministicID(d.GetRootCid(), d.GetPath())

	// Build OpenSearch document (must match mapping strict fields).
	doc := map[string]any{
		"doc_id":          docID,
		"root_cid":        d.GetRootCid(),
		"cid":             "",
		"path":            d.GetPath(),
		"path_text":       d.GetPath(),
		"filename":        d.GetFilename(),
		"filename_text":   d.GetFilename(),
		"node_type":       d.GetNodeType(),
		"ext":             d.GetExt(),
		"mime":            d.GetMime(),
		"size_bytes":      d.GetSizeBytes(),
		"content_indexed": d.GetContentIndexed(),
		"skip_reason":     "",
		"text":            d.GetText(),
		"text_truncated":  d.GetTextTruncated(),
		"names_text":      d.GetNamesText(),
		"discovered_at":   nil,
		"fetched_at":      nil,
		"processed_at":    d.GetProcessedAt(),
		"sources":         d.GetSources(),
		"ipns_name":       "",
		"dir": map[string]any{
			"entries_count":     0,
			"entries_truncated": false,
		},
	}

	b, _ := json.Marshal(doc)
	st, err := structpb.NewStruct(doc)
	if err != nil {
		// if structpb fails (e.g., unsupported types), fallback to raw json bytes by parsing into map
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		st, err = structpb.NewStruct(m)
		if err != nil {
			return err
		}
	}

	out := &ipfsnifferv1.IndexRequest{
		V:     1,
		Id:    uuid.NewString(),
		Ts:    time.Now().UTC().Format(time.RFC3339Nano),
		Trace: in.Trace,
		Data: &ipfsnifferv1.IndexRequestData{
			Index:    w.IndexName,
			DocId:    docID,
			Op:       "index",
			Document: st,
		},
	}

	payload, err := codec.Marshal(out)
	if err != nil {
		return err
	}

	if _, err := internalnats.Publish(ctx, w.NATS, internalnats.SubjectIndexRequest, payload); err != nil {
		_, _ = internalnats.PublishDLQ(ctx, w.NATS, internalnats.SubjectIndexRequest, payload)
		return err
	}
	return nil
}

func deterministicID(rootCID, p string) string {
	sum := sha256.Sum256([]byte(rootCID + ":" + p))
	return hex.EncodeToString(sum[:])
}
