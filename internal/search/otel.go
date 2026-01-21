package search

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "ipfsniffer/search"

func tracer() trace.Tracer {
	return otel.Tracer(tracerName)
}
