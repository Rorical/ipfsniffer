package logging

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type OTelConfig struct {
	Endpoint    string
	Insecure    bool
	ServiceName string
}

func InitOTel(ctx context.Context, cfg OTelConfig) (func(context.Context) error, error) {
	if cfg.Endpoint == "" {
		cfg.Endpoint = "127.0.0.1:4318"
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "ipfsniffer"
	}

	// Allow disabling OTel completely in resource-constrained deployments.
	if strings.TrimSpace(os.Getenv("IPFSNIFFER_OTEL_DISABLED")) == "1" {
		return func(context.Context) error { return nil }, nil
	}

	if v := strings.TrimSpace(os.Getenv("IPFSNIFFER_OTEL_ENDPOINT")); v != "" {
		cfg.Endpoint = v
	}

	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(cfg.Endpoint),
		otlptracehttp.WithTimeout(5 * time.Second),
	}
	if cfg.Insecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}

	exp, err := otlptracehttp.New(ctx, opts...)

	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
	)
	otel.SetTracerProvider(tp)

	// Ensure W3C trace context propagation for inbound HTTP requests.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}
