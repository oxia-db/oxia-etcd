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

// Command oxia-etcd serves the etcd v3 API over records kept in Oxia.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"hash/fnv"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/oxia-db/oxia/oxia"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.etcd.io/etcd/api/v3/version"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/oxia-db/oxia-etcd/internal/backend"
	"github.com/oxia-db/oxia-etcd/internal/server"
	"github.com/oxia-db/oxia-etcd/internal/store"
)

func main() {
	configPath := flag.String("config", "", "Path of the YAML configuration file")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		slog.Error("Failed to load the configuration", slog.Any("error", err))
		os.Exit(1)
	}
	level := &slog.LevelVar{}
	setLevel(level, cfg.Observability.Log.Level)
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	if err := run(log, level, *configPath, cfg); err != nil {
		log.Error("Exiting", slog.Any("error", err))
		os.Exit(1)
	}
}

func setLevel(level *slog.LevelVar, name string) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(name)); err != nil {
		l = slog.LevelInfo
	}
	level.Set(l)
}

func run(log *slog.Logger, level *slog.LevelVar, configPath string, cfg Config) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The metrics of the Oxia client, the gRPC server and the gateway, exported to Prometheus
	exporter, err := otelprometheus.New()
	if err != nil {
		return err
	}
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	defer func() { _ = meterProvider.Shutdown(context.Background()) }()

	be, err := backend.NewOxia(cfg.Oxia.ServiceAddress, cfg.Oxia.Namespace, cfg.Oxia.Cluster, cfg.Oxia.SessionTimeout,
		oxia.WithMeterProvider(meterProvider))
	if err != nil {
		return err
	}
	defer be.Close()

	st := store.New(be, store.Options{HistorySize: cfg.Server.HistorySize}, log)
	if err := registerMetrics(meterProvider.Meter("oxia-etcd"), st); err != nil {
		return err
	}
	srv := server.New(server.Config{
		ClusterID:        hash(cfg.Oxia.Cluster),
		MemberID:         hash(cfg.Oxia.Cluster + "/" + cfg.Server.Name),
		Name:             cfg.Server.Name,
		ClientURLs:       []string{cfg.Server.AdvertiseClientURL},
		ProgressInterval: cfg.Server.ProgressInterval,
	}, st)

	g := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithMeterProvider(meterProvider))),
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
		grpc.MaxConcurrentStreams(math.MaxUint32),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}),
	)
	srv.Register(g)
	grpcL, err := net.Listen("tcp", cfg.Server.BindAddress)
	if err != nil {
		return err
	}
	httpL, err := net.Listen("tcp", cfg.Observability.Metric.BindAddress)
	if err != nil {
		return err
	}
	h := &http.Server{Handler: httpHandler(st, cfg.Observability.Metric.Enabled), ReadHeaderTimeout: 10 * time.Second}
	if cfg.Observability.Pprof.Enabled {
		pprofServer, err := servePprof(log, cfg.Observability.Pprof.BindAddress)
		if err != nil {
			return err
		}
		defer pprofServer.Close()
	}
	if configPath != "" {
		if err := watchConfig(ctx, log, configPath, func(next Config) {
			setLevel(level, next.Observability.Log.Level)
			srv.SetProgressInterval(next.Server.ProgressInterval)
		}); err != nil {
			return err
		}
	}

	errC := make(chan error, 3)
	go func() { errC <- g.Serve(grpcL) }()
	go func() { errC <- h.Serve(httpL) }()
	go func() { errC <- st.Run(ctx) }()
	log.Info("Serving the etcd API", slog.String("listen", cfg.Server.BindAddress), slog.String("http", cfg.Observability.Metric.BindAddress),
		slog.String("oxia", cfg.Oxia.ServiceAddress), slog.String("namespace", cfg.Oxia.Namespace),
		slog.String("cluster", cfg.Oxia.Cluster))

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

func registerMetrics(meter metric.Meter, st *store.Store) error {
	revision, err := meter.Int64ObservableGauge("oxia_etcd_revision", metric.WithDescription("Current etcd revision"))
	if err != nil {
		return err
	}
	ready, err := meter.Int64ObservableGauge("oxia_etcd_ready",
		metric.WithDescription("1 when the gateway owns the records and serves them, 0 on standby"))
	if err != nil {
		return err
	}
	size, err := meter.Int64ObservableGauge("oxia_etcd_size", metric.WithUnit("By"),
		metric.WithDescription("Estimated size of the keys and values"))
	if err != nil {
		return err
	}
	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(revision, st.Rev())
		o.ObserveInt64(size, st.Size())
		if st.Ready() {
			o.ObserveInt64(ready, 1)
		} else {
			o.ObserveInt64(ready, 0)
		}
		return nil
	}, revision, ready, size)
	return err
}

// httpHandler serves the metrics, and the etcd HTTP endpoints that tools check.
func httpHandler(st *store.Store, metrics bool) http.Handler {
	mux := http.NewServeMux()
	writeJSON := func(w http.ResponseWriter, code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	if metrics {
		mux.Handle("/metrics", promhttp.Handler())
	}
	mux.HandleFunc("/version", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{
			"etcdserver":  version.Version,
			"etcdcluster": version.Cluster(version.Version),
		})
	})
	health := func(w http.ResponseWriter, _ *http.Request) {
		if st.Ready() {
			writeJSON(w, http.StatusOK, map[string]string{"health": "true", "reason": ""})
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"health": "false", "reason": "standby"})
	}
	mux.HandleFunc("/health", health)
	mux.HandleFunc("/readyz", health)
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	return mux
}

// servePprof serves the Go profiles, with the mutex and block profiles sampled.
func servePprof(log *slog.Logger, bindAddress string) (*http.Server, error) {
	l, err := net.Listen("tcp", bindAddress)
	if err != nil {
		return nil, err
	}
	runtime.SetMutexProfileFraction(100)
	runtime.SetBlockProfileRate(100_000)
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	s := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = s.Serve(l) }()
	log.Info("Serving the Go profiles", slog.String("url", "http://"+bindAddress+"/debug/pprof"))
	return s, nil
}

func hash(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}
