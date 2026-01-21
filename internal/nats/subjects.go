package nats

const (
	StreamName = "IPFSNIFFER"

	SubjectCidDiscovered = "cid.discovered"
	SubjectFetchRequest  = "fetch.request"
	SubjectFetchResult   = "fetch.result"
	SubjectDocReady      = "doc.ready"
	SubjectIndexRequest  = "index.request"

	SubjectStreamGet         = "stream.get"
	SubjectStreamChunkPrefix = "stream.chunk."
)

var PipelineSubjects = []string{
	SubjectCidDiscovered,
	SubjectFetchRequest,
	SubjectFetchResult,
	SubjectDocReady,
	SubjectIndexRequest,
	SubjectStreamGet,
	SubjectStreamChunkPrefix + "*",
}

func StreamChunkSubject(streamID string) string {
	return SubjectStreamChunkPrefix + streamID
}

func DLQSubject(subject string) string {
	return subject + ".dlq"
}
