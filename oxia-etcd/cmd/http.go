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

package cmd

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.etcd.io/etcd/api/v3/version"
	"go.opentelemetry.io/otel/metric"

	"github.com/oxia-db/oxia-etcd/internal/store"
)

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
