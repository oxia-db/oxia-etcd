// Copyright 2023-2026 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package cmd is the oxia-etcd command: it serves the etcd v3 API over
// records kept in Oxia, with the configuration of a YAML file.
package cmd

import (
	"context"
	"errors"
	"flag"
	"hash/fnv"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/oxia-db/oxia/oxia"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/oxia-db/oxia-etcd/internal/backend"
	"github.com/oxia-db/oxia-etcd/internal/server"
	"github.com/oxia-db/oxia-etcd/internal/store"
	"github.com/oxia-db/oxia-etcd/oxia-etcd/configwatch"
	"github.com/oxia-db/oxia-etcd/oxia-etcd/option"
)

// Execute parses the flags, and serves until a signal or a failure. It logs
// the error it returns.
func Execute() error {
	// The flag of the configuration file of an Oxia server
	configPath := flag.String("sconfig", "", "Path of the YAML configuration file")
	flag.Parse()

	options, err := option.Load(*configPath)
	if err != nil {
		slog.Error("Failed to load the configuration", slog.Any("error", err))
		return err
	}
	level := &slog.LevelVar{}
	setLevel(level, options.Observability.Log)
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	if err := run(log, level, *configPath, options); err != nil {
		log.Error("Exiting", slog.Any("error", err))
		return err
	}
	return nil
}

// setLevel sets the level of validated options.
func setLevel(level *slog.LevelVar, options option.LogOptions) {
	l, _ := options.SlogLevel()
	level.Set(l)
}

func run(log *slog.Logger, level *slog.LevelVar, configPath string, options *option.Options) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The metrics of the Oxia client, the gRPC server and the gateway, exported to Prometheus
	exporter, err := otelprometheus.New()
	if err != nil {
		return err
	}
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	defer func() { _ = meterProvider.Shutdown(context.Background()) }()

	be, err := backend.NewOxia(options.Oxia.ServiceAddress, options.Oxia.Namespace, options.Oxia.Cluster, options.Oxia.SessionTimeout,
		oxia.WithMeterProvider(meterProvider))
	if err != nil {
		return err
	}
	defer be.Close()

	st := store.New(be, store.Options{HistorySize: options.Server.HistorySize}, log)
	if err := registerMetrics(meterProvider.Meter("oxia-etcd"), st); err != nil {
		return err
	}
	srv := server.New(server.Config{
		ClusterID:        hash(options.Oxia.Cluster),
		MemberID:         hash(options.Oxia.Cluster + "/" + options.Server.Name),
		Name:             options.Server.Name,
		ClientURLs:       []string{options.Server.AdvertiseClientURL},
		ProgressInterval: options.Server.ProgressInterval,
	}, st)

	g := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithMeterProvider(meterProvider))),
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
		grpc.MaxConcurrentStreams(math.MaxUint32),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}),
	)
	srv.Register(g)
	grpcL, err := net.Listen("tcp", options.Server.BindAddress)
	if err != nil {
		return err
	}
	httpL, err := net.Listen("tcp", options.Observability.Metric.BindAddress)
	if err != nil {
		return err
	}
	h := &http.Server{Handler: httpHandler(st, options.Observability.Metric.IsEnabled()), ReadHeaderTimeout: 10 * time.Second}
	if options.Observability.Pprof.IsEnabled() {
		pprofServer, err := servePprof(log, options.Observability.Pprof.BindAddress)
		if err != nil {
			return err
		}
		defer pprofServer.Close()
	}
	if configPath != "" {
		if err := configwatch.Watch(ctx, log, configPath, option.Load, func(next *option.Options) {
			setLevel(level, next.Observability.Log)
			srv.SetProgressInterval(next.Server.ProgressInterval)
		}); err != nil {
			return err
		}
	}

	errC := make(chan error, 3)
	go func() { errC <- g.Serve(grpcL) }()
	go func() { errC <- h.Serve(httpL) }()
	go func() { errC <- st.Run(ctx) }()
	log.Info("Serving the etcd API", slog.String("listen", options.Server.BindAddress), slog.String("http", options.Observability.Metric.BindAddress),
		slog.String("oxia", options.Oxia.ServiceAddress), slog.String("namespace", options.Oxia.Namespace),
		slog.String("cluster", options.Oxia.Cluster))

	select {
	case <-ctx.Done():
		err = nil
	case err = <-errC:
	}
	g.Stop()
	_ = h.Close()
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return err
}

func hash(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}
