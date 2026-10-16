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

// Package oxiaetcd is the oxia-etcd command: it serves the etcd v3 API over
// records kept in Oxia, with the configuration of a YAML file.
package oxiaetcd

import (
	"context"
	"errors"
	"flag"
	"hash/fnv"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/oxia"
	"github.com/spf13/viper"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/oxia-db/oxia-etcd/internal/destination"
	"github.com/oxia-db/oxia-etcd/internal/option"
	"github.com/oxia-db/oxia-etcd/internal/server"
	"github.com/oxia-db/oxia-etcd/internal/store"
)

// Execute parses the flags, and serves until a signal, as an Oxia server
// does: it exits the process when it is done.
func Execute() {
	// The flag of the configuration file of an Oxia server
	configPath := flag.String("sconfig", "", "Path of the YAML configuration file")
	flag.Parse()

	options, err := option.Load(*configPath)
	if err != nil {
		slog.Error("Failed to load the configuration", slog.Any("error", err))
		os.Exit(1)
	}
	level := &slog.LevelVar{}
	setLevel(level, options.Observability.Log)
	// The default logger, so that Oxia's process helpers log as the gateway does
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	process.PprofEnable = options.Observability.Pprof.IsEnabled()
	process.PprofBindAddress = options.Observability.Pprof.BindAddress
	process.RunProcess(func() (io.Closer, error) {
		return start(level, *configPath, options)
	})
}

// setLevel sets the level of validated options.
func setLevel(level *slog.LevelVar, options option.LogOptions) {
	l, _ := options.SlogLevel()
	level.Set(l)
}

// gateway is the running gateway, which Oxia's process closes on a signal.
type gateway struct {
	grpc          *grpc.Server
	http          *http.Server
	cancel        context.CancelFunc
	storeDone     chan struct{}
	destination   destination.Destination
	meterProvider *sdkmetric.MeterProvider
	closing       chan struct{}
}

func start(level *slog.LevelVar, configPath string, options *option.Options) (*gateway, error) {
	log := slog.Default()

	// The metrics of the Oxia client, the gRPC server and the gateway, exported to Prometheus
	exporter, err := otelprometheus.New()
	if err != nil {
		return nil, err
	}
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))

	dst, err := destination.NewOxia(options.Oxia.ServiceAddress, options.Oxia.Namespace, options.Oxia.Cluster,
		options.Oxia.SessionTimeout, oxia.WithMeterProvider(meterProvider))
	if err != nil {
		return nil, err
	}

	st := store.New(dst, store.Options{HistorySize: options.Server.HistorySize}, log)
	if err := registerMetrics(meterProvider.Meter("oxia-etcd"), st); err != nil {
		return nil, err
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
	httpL, err := net.Listen("tcp", options.Observability.Metric.BindAddress)
	if err != nil {
		return nil, err
	}
	h := &http.Server{Handler: httpHandler(st, options.Observability.Metric.IsEnabled()), ReadHeaderTimeout: 10 * time.Second}

	if configPath != "" {
		// As Oxia's server: viper follows the file, also when it is replaced,
		// as Kubernetes does with the files of a ConfigMap
		v := viper.New()
		v.SetConfigFile(configPath)
		v.OnConfigChange(func(fsnotify.Event) {
			next, err := option.Load(configPath)
			if err != nil {
				log.Warn("Ignoring the invalid configuration", slog.String("path", configPath), slog.Any("error", err))
				return
			}
			log.Info("Reloaded the configuration", slog.String("path", configPath))
			setLevel(level, next.Observability.Log)
			srv.SetProgressInterval(next.Server.ProgressInterval)
		})
		v.WatchConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())
	gw := &gateway{
		grpc:          g,
		http:          h,
		cancel:        cancel,
		storeDone:     make(chan struct{}),
		destination:   dst,
		meterProvider: meterProvider,
		closing:       make(chan struct{}),
	}
	go gw.serveWhenOwner(st, options.Server.BindAddress)
	go func() { gw.exitOnFailure("HTTP", h.Serve(httpL)) }()
	go func() {
		defer close(gw.storeDone)
		// Losing the ownership of the records is fatal: another gateway serves them
		gw.exitOnFailure("store", st.Run(ctx))
	}()
	log.Info("Started the gateway", slog.String("listen", options.Server.BindAddress),
		slog.String("http", options.Observability.Metric.BindAddress), slog.String("oxia", options.Oxia.ServiceAddress),
		slog.String("namespace", options.Oxia.Namespace), slog.String("cluster", options.Oxia.Cluster))
	return gw, nil
}

// serveWhenOwner opens the etcd port once the gateway owns the records. A
// standby keeps it closed: unlike an etcd follower, it can't forward the
// requests, and etcd clients send them only to the endpoints they can reach.
func (gw *gateway) serveWhenOwner(st *store.Store, bindAddress string) {
	select {
	case <-st.Serving():
	case <-gw.closing:
		return
	}
	l, err := net.Listen("tcp", bindAddress)
	if err != nil {
		gw.exitOnFailure("etcd API", err)
		return
	}
	slog.Info("Serving the etcd API", slog.String("listen", bindAddress))
	gw.exitOnFailure("etcd API", gw.grpc.Serve(l))
}

// exitOnFailure exits the process when a part of the gateway stops while it
// is not closing.
func (gw *gateway) exitOnFailure(part string, err error) {
	select {
	case <-gw.closing:
		return
	default:
	}
	if errors.Is(err, http.ErrServerClosed) {
		return
	}
	slog.Error("The gateway stopped", slog.String("part", part), slog.Any("error", err))
	os.Exit(1)
}

// Close stops serving, then gives up the ownership of the records, so that a
// standby gateway takes over without waiting for the session to expire.
func (gw *gateway) Close() error {
	close(gw.closing)
	gw.grpc.Stop()
	err := gw.http.Close()
	gw.cancel()
	<-gw.storeDone
	return errors.Join(err, gw.destination.Close(), gw.meterProvider.Shutdown(context.Background()))
}

func hash(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}
