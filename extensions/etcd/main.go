package main

import (
	"context"
	"errors"
	gc "khanh/raft-go/common"
	"khanh/raft-go/extensions/etcd/common"
	"khanh/raft-go/extensions/etcd/node"
	"khanh/raft-go/observability"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.opentelemetry.io/otel"
)

func main() {
	ctx := context.Background()
	tracer := otel.Tracer("main.go")
	ctx, span := tracer.Start(ctx, "main.go")

	config, err := common.ReadConfigFromFile(nil)
	if err != nil {
		log.Panic(err)
	}

	nId, err := gc.ReadCmdFlags(ctx, config.RaftCore.Cluster.Mode, config.RaftCore.Cluster.Servers)
	if err != nil {
		log.Panic(err)
	}

	logger := observability.NewZerolog(config.Observability, nId.ID)
	logger.InfoContext(ctx, "config content", "config", config)

	params, err := node.PrepareNewNodeParams(ctx, nId.ID, nId.HttpUrl, nId.RpcUrl, nId.CatchingUp, config)
	if err != nil {
		log.Panic(err)
	}

	n := node.NewNode(ctx, params)

	n.Start(ctx)

	// Set up OpenTelemetry.
	if !config.Observability.Disabled {
		otelShutdown, err := observability.SetupOTelSDK(ctx, nId.ID, config.Observability)
		if err != nil {
			log.Panic(err)
		}

		defer func() {
			err = errors.Join(err, otelShutdown(ctx))
			log.Panic(err)
		}()
	}

	span.End() // end init node

	signChan := make(chan os.Signal, 1)
	signal.Notify(signChan, os.Interrupt, syscall.SIGTERM)
	<-signChan
	logger.Info("shutdown")
}
