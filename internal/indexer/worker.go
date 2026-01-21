package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Rorical/IPFSniffer/internal/codec"
	"github.com/Rorical/IPFSniffer/internal/logging"
	internalnats "github.com/Rorical/IPFSniffer/internal/nats"
	"github.com/Rorical/IPFSniffer/internal/opensearch"
	ipfsnifferv1 "github.com/Rorical/IPFSniffer/proto"

	nats "github.com/nats-io/nats.go"
	osclient "github.com/opensearch-project/opensearch-go/v4"
	osapi "github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

type Worker struct {
	NATS nats.JetStreamContext
	OS   *osclient.Client

	Durable    string
	MaxDeliver int

	BulkMax       int
	FlushInterval time.Duration
}

func (w *Worker) Run(ctx context.Context) error {
	if w.NATS == nil {
		return fmt.Errorf("nats required")
	}
	if w.OS == nil {
		return fmt.Errorf("opensearch client required")
	}
	if w.BulkMax <= 0 {
		w.BulkMax = 100
	}
	if w.FlushInterval <= 0 {
		w.FlushInterval = 2 * time.Second
	}
	if w.Durable == "" {
		w.Durable = "indexer"
	}

	if err := internalnats.EnsureConsumer(ctx, w.NATS, internalnats.SubjectIndexRequest, w.Durable, w.MaxDeliver); err != nil {
		return err
	}

	sub, err := w.NATS.PullSubscribe(internalnats.SubjectIndexRequest, w.Durable)
	if err != nil {
		return err
	}

	logger := logging.FromContext(ctx)
	logger.Info("indexer started", "subject", internalnats.SubjectIndexRequest, "durable", w.Durable)

	var batch []*nats.Msg
	lastFlush := time.Now()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		ackMsgs, err := w.flushBatch(ctx, batch)
		if err != nil {
			return err
		}
		if len(ackMsgs) != len(batch) {
			return fmt.Errorf("flushBatch: ack count mismatch: got %d want %d", len(ackMsgs), len(batch))
		}

		for _, m := range ackMsgs {
			_ = m.Ack()
		}
		batch = batch[:0]
		lastFlush = time.Now()
		return nil
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		msgs, err := sub.Fetch(1, nats.MaxWait(200*time.Millisecond))
		if err != nil {
			if err == nats.ErrTimeout {
				if time.Since(lastFlush) >= w.FlushInterval {
					if err := flush(); err != nil {
						logger.Error("flush", "err", err)
					}
				}
				continue
			}
			return err
		}

		batch = append(batch, msgs...)
		if len(batch) >= w.BulkMax {
			if err := flush(); err != nil {
				logger.Error("flush", "err", err)
			}
		}
	}
}

func (w *Worker) flushBatch(ctx context.Context, msgs []*nats.Msg) ([]*nats.Msg, error) {
	logger := logging.FromContext(ctx)

	type bulkItem struct {
		msg *nats.Msg
		id  string
	}

	items := make([]bulkItem, 0, len(msgs))
	var body bytes.Buffer

	for _, m := range msgs {
		var in ipfsnifferv1.IndexRequest
		if err := codec.Unmarshal(m.Data, &in); err != nil {
			// Malformed message; DLQ and continue.
			_, _ = internalnats.PublishDLQ(ctx, w.NATS, internalnats.SubjectIndexRequest, m.Data)
			items = append(items, bulkItem{msg: m, id: ""})
			continue
		}
		d := in.GetData()
		if d == nil {
			items = append(items, bulkItem{msg: m, id: ""})
			continue
		}

		items = append(items, bulkItem{msg: m, id: d.GetDocId()})

		meta := map[string]any{"index": map[string]any{"_index": d.GetIndex(), "_id": d.GetDocId()}}
		mb, _ := json.Marshal(meta)
		body.Write(mb)
		body.WriteByte('\n')

		// Document is google.protobuf.Struct
		doc := d.GetDocument().AsMap()
		db, _ := json.Marshal(doc)
		body.Write(db)
		body.WriteByte('\n')
	}

	// Use opensearchapi Bulk endpoint.
	client := osapi.Client{Client: w.OS}
	resp, err := client.Bulk(ctx, osapi.BulkReq{Body: bytes.NewReader(body.Bytes())})
	if err != nil {
		return nil, err
	}

	// If OpenSearch itself returned non-2xx, treat as transient; do not ack.
	if resp.Inspect().Response != nil {
		code := resp.Inspect().Response.StatusCode
		if code < 200 || code >= 300 {
			return nil, fmt.Errorf("bulk http status %d", code)
		}
	}

	// Happy path: ack everything.
	if !resp.Errors {
		acks := make([]*nats.Msg, 0, len(items))
		for _, it := range items {
			acks = append(acks, it.msg)
		}
		return acks, nil
	}

	// Errors=true: OpenSearch accepted request but some items failed. DLQ only those items.
	if len(resp.Items) != len(items) {
		// Unexpected shape: fail the batch so JetStream retries; safer than acking blindly.
		return nil, fmt.Errorf("bulk items mismatch: got %d want %d", len(resp.Items), len(items))
	}

	acks := make([]*nats.Msg, 0, len(items))
	failed := 0
	for i := range items {
		// Each entry is like {"index": {...}}
		var item osapi.BulkRespItem
		for _, v := range resp.Items[i] {
			item = v
			break
		}

		if item.Status >= 200 && item.Status < 300 {
			acks = append(acks, items[i].msg)
			continue
		}

		failed++
		// DLQ the original IndexRequest payload for inspection/replay.
		_, _ = internalnats.PublishDLQ(ctx, w.NATS, internalnats.SubjectIndexRequest, items[i].msg.Data)
		acks = append(acks, items[i].msg)

		errType := ""
		errReason := ""
		if item.Error != nil {
			errType = item.Error.Type
			errReason = item.Error.Reason
		}
		logger.Error("bulk item failed", "doc_id", items[i].id, "status", item.Status, "err_type", errType, "err_reason", errReason)
	}

	logger.Warn("bulk had item failures", "failed", failed, "total", len(items))
	return acks, nil
}

// Optional helper: ensure index exists on startup.
func EnsureDefaultIndex(ctx context.Context, c *osclient.Client, indexName string) error {
	spec := opensearch.IndexSpec{IndexName: indexName, AliasName: "ipfsniffer-docs"}
	return opensearch.EnsureIndex(ctx, c, spec, opensearch.DefaultMappingJSON)
}
