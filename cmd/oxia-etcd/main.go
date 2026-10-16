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
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/soheilhy/cmux"
	"go.etcd.io/etcd/api/v3/version"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/oxia-db/oxia-etcd/internal/backend"
	"github.com/oxia-db/oxia-etcd/internal/server"
	"github.com/oxia-db/oxia-etcd/internal/store"
)

func main() {
	var (
		listen           = flag.String("listen", "0.0.0.0:2379", "Address to serve the etcd API on")
		advertise        = flag.String("advertise-client-url", "http://127.0.0.1:2379", "Client URL the gateway advertises")
		name             = flag.String("name", "oxia-etcd", "Member name the gateway reports")
		oxiaAddress      = flag.String("oxia", "localhost:6648", "Oxia service address")
		namespace        = flag.String("namespace", "default", "Oxia namespace of the records")
		cluster          = flag.String("cluster", "default", "Name of the etcd cluster: its records share one Oxia shard")
		sessionTimeout   = flag.Duration("session-timeout", 10*time.Second, "Oxia session timeout of the gateway's ownership")
		historySize      = flag.Int("history-size", 100_000, "Number of recent events kept for watchers and past reads")
		progressInterval = flag.Duration("progress-interval", 5*time.Second, "Interval of the watch progress notifications")
		logLevel         = flag.String("log-level", "info", "Log level: debug, info, warn or error")
	)
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		level = slog.LevelInfo
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	if err := run(log, *listen, *advertise, *name, *oxiaAddress, *namespace, *cluster, *sessionTimeout,
		*historySize, *progressInterval); err != nil {
		log.Error("Exiting", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(log *slog.Logger, listen, advertise, name, oxiaAddress, namespace, cluster string,
	sessionTimeout time.Duration, historySize int, progressInterval time.Duration) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	be, err := backend.NewOxia(oxiaAddress, namespace, cluster, sessionTimeout)
	if err != nil {
		return err
	}
	defer be.Close()

	st := store.New(be, store.Options{HistorySize: historySize}, log)
	srv := server.New(server.Config{
		ClusterID:        hash(cluster),
		MemberID:         hash(cluster + "/" + name),
		Name:             name,
		ClientURLs:       []string{advertise},
		ProgressInterval: progressInterval,
	}, st)

	g := grpc.NewServer(
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
		grpc.MaxConcurrentStreams(math.MaxUint32),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}),
	)
	srv.Register(g)

	l, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	m := cmux.New(l)
	httpL := m.Match(cmux.HTTP1Fast())
	grpcL := m.Match(cmux.Any())
	h := &http.Server{Handler: httpHandler(st), ReadHeaderTimeout: 10 * time.Second}

	errC := make(chan error, 4)
	go func() { errC <- g.Serve(grpcL) }()
	go func() { errC <- h.Serve(httpL) }()
	go func() { errC <- m.Serve() }()
	go func() { errC <- st.Run(ctx) }()
	log.Info("Serving the etcd API", slog.String("listen", listen), slog.String("oxia", oxiaAddress),
		slog.String("namespace", namespace), slog.String("cluster", cluster))

	select {
	case <-ctx.Done():
		err = nil
	case err = <-errC:
	}
	g.Stop()
	_ = h.Close()
	if errors.Is(err, cmux.ErrListenerClosed) || errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return err
}

// httpHandler serves the etcd HTTP endpoints that tools check, such as kubeadm's version check.
func httpHandler(st *store.Store) http.Handler {
	mux := http.NewServeMux()
	writeJSON := func(w http.ResponseWriter, code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
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

func hash(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}
