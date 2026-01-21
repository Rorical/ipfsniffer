package opensearch

// DefaultMappingJSON is the initial index settings + mappings.
// Keep in sync with AGENTS.md mapping section.
var DefaultMappingJSON = []byte(`{
  "settings": {
    "index": {
      "number_of_shards": 3,
      "number_of_replicas": 1,
      "refresh_interval": "5s"
    }
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "doc_id": { "type": "keyword" },
      "root_cid": { "type": "keyword" },
      "cid": { "type": "keyword" },
      "path": { "type": "keyword" },
      "path_text": { "type": "text" },
      "filename": { "type": "keyword" },
      "filename_text": { "type": "search_as_you_type" },
      "node_type": { "type": "keyword" },
      "ext": { "type": "keyword" },
      "mime": { "type": "keyword" },
      "size_bytes": { "type": "long" },
      "content_indexed": { "type": "boolean" },
      "skip_reason": { "type": "keyword" },
      "text": { "type": "text" },
      "text_truncated": { "type": "boolean" },
      "names_text": { "type": "text" },
      "discovered_at": { "type": "date" },
      "fetched_at": { "type": "date" },
      "processed_at": { "type": "date" },
      "sources": { "type": "keyword" },
      "ipns_name": { "type": "keyword" },
      "dir": {
        "properties": {
          "entries_count": { "type": "integer" },
          "entries_truncated": { "type": "boolean" }
        }
      }
    }
  }
}`)
