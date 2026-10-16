package main

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the configuration of the gateway, read from a YAML file. The
// gateway watches the file: the log level and the progress interval change
// as soon as the file does, and the other settings on the next restart.
type Config struct {
	Server        ServerConfig        `yaml:"server"`
	Oxia          OxiaConfig          `yaml:"oxia"`
	Observability ObservabilityConfig `yaml:"observability"`
}

type ServerConfig struct {
	// BindAddress is the address of the etcd API.
	BindAddress string `yaml:"bindAddress"`
	// AdvertiseClientURL is the client URL the gateway reports.
	AdvertiseClientURL string `yaml:"advertiseClientURL"`
	// Name is the member name the gateway reports.
	Name string `yaml:"name"`
	// HistorySize is the number of recent events kept for watchers and past reads.
	HistorySize int `yaml:"historySize"`
	// ProgressInterval is the interval of the watch progress notifications.
	ProgressInterval time.Duration `yaml:"progressInterval"`
}

type OxiaConfig struct {
	// ServiceAddress is the address of the Oxia service.
	ServiceAddress string `yaml:"serviceAddress"`
	// Namespace is the Oxia namespace of the records.
	Namespace string `yaml:"namespace"`
	// Cluster is the name of the etcd cluster: its records share one Oxia shard.
	Cluster string `yaml:"cluster"`
	// SessionTimeout is the Oxia session timeout of the gateway's ownership.
	SessionTimeout time.Duration `yaml:"sessionTimeout"`
}

type ObservabilityConfig struct {
	Metric MetricConfig `yaml:"metric"`
	Log    LogConfig    `yaml:"log"`
	Pprof  PprofConfig  `yaml:"pprof"`
}

type MetricConfig struct {
	// Enabled serves the Prometheus metrics on /metrics.
	Enabled bool `yaml:"enabled"`
	// BindAddress is the address of /metrics, and of /health, /readyz, /livez
	// and /version, which are served even with the metrics disabled.
	BindAddress string `yaml:"bindAddress"`
}

type LogConfig struct {
	// Level is debug, info, warn or error.
	Level string `yaml:"level"`
}

type PprofConfig struct {
	// Enabled serves the Go profiles on /debug/pprof.
	Enabled bool `yaml:"enabled"`
	// BindAddress is the address of /debug/pprof.
	BindAddress string `yaml:"bindAddress"`
}

func defaultConfig() Config {
	return Config{
		Server: ServerConfig{
			BindAddress:        "0.0.0.0:2379",
			AdvertiseClientURL: "http://127.0.0.1:2379",
			Name:               "oxia-etcd",
			HistorySize:        100_000,
			ProgressInterval:   5 * time.Second,
		},
		Oxia: OxiaConfig{
			ServiceAddress: "localhost:6648",
			Namespace:      "default",
			Cluster:        "default",
			SessionTimeout: 10 * time.Second,
		},
		Observability: ObservabilityConfig{
			Metric: MetricConfig{Enabled: true, BindAddress: "0.0.0.0:8080"},
			Log:    LogConfig{Level: "info"},
			Pprof:  PprofConfig{Enabled: true, BindAddress: "127.0.0.1:6060"},
		},
	}
}

// loadConfig reads the configuration at path over the defaults. An empty path gives the defaults.
func loadConfig(path string) (Config, error) {
	cfg := defaultConfig()
	if path == "" {
		return cfg, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	err = yaml.Unmarshal(data, &cfg)
	return cfg, err
}
