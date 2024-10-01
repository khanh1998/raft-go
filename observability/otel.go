package observability

import (
	"context"
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutlog"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

var (
	meter = otel.Meter("raft")

	requestVotesDuration  metric.Int64Histogram
	appendEntriesDuration metric.Int64Histogram
	MetricsExporter       prometheus.Exporter
)

func init() {
	var err error

	requestVotesDuration, err = meter.Int64Histogram(
		"LeaderElectionDuration",
		metric.WithDescription("how long does it take to finish a successful election round"),
		metric.WithUnit("{ms}"),
	)

	if err != nil {
		log.Println(err.Error())
	}

	appendEntriesDuration, err = meter.Int64Histogram(
		"AppendEntriesDuration",
		metric.WithDescription("how long does it take to finish a successful append entries round"),
		metric.WithUnit("{ms}"),
	)

	if err != nil {
		log.Println(err.Error())
	}
}

func SetRequestVoteDuration(ctx context.Context, dur time.Duration) {
	requestVotesDuration.Record(ctx, dur.Milliseconds())
}

func SetAppendEntriesDuration(ctx context.Context, dur time.Duration) {
	appendEntriesDuration.Record(ctx, dur.Milliseconds())
}

func SetupOTelSDK(ctx context.Context, nodeId int, cfg common.ObservabilityConfig) (shutdown func(context.Context) error, err error) {
	var shutdownFuncs []func(context.Context) error

	// shutdown calls cleanup functions registered via shutdownFuncs.
	// The errors from the calls are joined.
	// Each registered cleanup will be invoked once.
	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}

	// handleErr calls shutdown for cleanup and makes sure that all errors are returned.
	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}

	// Set up propagator.
	prop := newPropagator()
	otel.SetTextMapPropagator(prop)

	// Set up trace provider.
	tracerProvider, err := newTraceProvider(nodeId, cfg.TraceEndpoint)
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
	otel.SetTracerProvider(tracerProvider)

	// Set up meter provider.
	meterProvider, err := newMeterProvider()
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)
	otel.SetMeterProvider(meterProvider)

	// Set up logger provider.
	loggerProvider, err := newLoggerProvider(ctx, cfg.LogEndpoint, nodeId)
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, loggerProvider.Shutdown)
	global.SetLoggerProvider(loggerProvider)

	return
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

func newTraceProvider(nodeId int, traceSever string) (*trace.TracerProvider, error) {
	exporter, err := otlptracehttp.New(context.Background(), otlptracehttp.WithEndpoint(traceSever), otlptracehttp.WithInsecure())
	if err != nil {
		return nil, err
	}

	// Create the trace provider
	tracerProvider := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(fmt.Sprintf("raft-node-%d", nodeId)),
		)),
	)

	return tracerProvider, nil
}

func newMeterProvider() (*sdkmetric.MeterProvider, error) {
	metricExporter, err := prometheus.New()
	if err != nil {
		return nil, err
	}
	metricExporter = metricExporter

	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricExporter))

	return meterProvider, nil
}

func newLoggerProvider(ctx context.Context, logServer string, nodeId int) (*sdklog.LoggerProvider, error) {
	logExporter, err := stdoutlog.New()
	if err != nil {
		return nil, err
	}

	otlpExporter, err := otlploghttp.New(ctx, otlploghttp.WithEndpoint(logServer), otlploghttp.WithURLPath("/otlp/v1/logs"), otlploghttp.WithInsecure())
	if err != nil {
		return nil, err
	}

	loggerProvider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(otlpExporter)),
		sdklog.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(fmt.Sprintf("raft-node-%d", nodeId)),
		)),
	)
	return loggerProvider, nil
}
