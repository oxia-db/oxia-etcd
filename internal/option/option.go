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

// Package option is the configuration of the gateway, read from a YAML file,
// with the option pattern of Oxia: WithDefault fills the settings that the
// file leaves out, and Validate rejects the invalid ones.
//
// The gateway watches the file: the log level and the progress interval
// change as soon as the file does, and the other settings on the next restart.
package option

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/oxia-db/oxia/common/codec"
)

type Options struct {
	Server        ServerOptions        `yaml:"server" json:"server" jsonschema:"description=etcd API served by the gateway"`
	Oxia          OxiaOptions          `yaml:"oxia" json:"oxia" jsonschema:"description=Oxia service that keeps the records"`
	Observability ObservabilityOptions `yaml:"observability" json:"observability" jsonschema:"description=Metrics and logs and Go profiles"`
}

func (o *Options) WithDefault() {
	o.Server.WithDefault()
	o.Oxia.WithDefault()
	o.Observability.WithDefault()
}

func (o *Options) Validate() error {
	return errors.Join(
		o.Server.Validate(),
		o.Oxia.Validate(),
		o.Observability.Validate(),
	)
}

func NewDefaultOptions() *Options {
	o := &Options{}
	o.WithDefault()
	return o
}

// Load reads the options at path, fills the defaults and validates them. An
// empty path gives the defaults.
func Load(path string) (*Options, error) {
	if path == "" {
		o := NewDefaultOptions()
		return o, o.Validate()
	}
	o := &Options{}
	if err := codec.TryReadAndInitConf(path, o); err != nil {
		return nil, err
	}
	return o, nil
}

type ServerOptions struct {
	BindAddress        string        `yaml:"bindAddress" json:"bindAddress" jsonschema:"description=Address of the etcd API,example=0.0.0.0:2379,format=hostname"`
	AdvertiseClientURL string        `yaml:"advertiseClientURL" json:"advertiseClientURL" jsonschema:"description=Client URL the gateway reports,example=http://127.0.0.1:2379"`
	Name               string        `yaml:"name" json:"name" jsonschema:"description=Member name the gateway reports,default=oxia-etcd"`
	HistorySize        int           `yaml:"historySize" json:"historySize" jsonschema:"description=Number of recent events kept for watchers and past reads,default=100000,minimum=1"`
	ProgressInterval   time.Duration `yaml:"progressInterval" json:"progressInterval" jsonschema:"description=Interval of the watch progress notifications,example=5s,format=duration"`
}

func (so *ServerOptions) WithDefault() {
	if so.BindAddress == "" {
		so.BindAddress = "0.0.0.0:2379"
	}
	if so.AdvertiseClientURL == "" {
		so.AdvertiseClientURL = "http://127.0.0.1:2379"
	}
	if so.Name == "" {
		so.Name = "oxia-etcd"
	}
	if so.HistorySize == 0 {
		so.HistorySize = 100_000
	}
	if so.ProgressInterval == 0 {
		so.ProgressInterval = 5 * time.Second
	}
}

func (so *ServerOptions) Validate() error {
	return errors.Join(
		notEmpty("server.bindAddress", so.BindAddress),
		notEmpty("server.advertiseClientURL", so.AdvertiseClientURL),
		notEmpty("server.name", so.Name),
		positive("server.historySize", so.HistorySize),
		positive("server.progressInterval", so.ProgressInterval),
	)
}

type OxiaOptions struct {
	ServiceAddress string        `yaml:"serviceAddress" json:"serviceAddress" jsonschema:"description=Address of the Oxia service,example=localhost:6648,format=hostname"`
	Namespace      string        `yaml:"namespace" json:"namespace" jsonschema:"description=Oxia namespace of the records,default=default"`
	Cluster        string        `yaml:"cluster" json:"cluster" jsonschema:"description=Name of the etcd cluster: its records share one Oxia shard,default=default"`
	SessionTimeout time.Duration `yaml:"sessionTimeout" json:"sessionTimeout" jsonschema:"description=Oxia session timeout of the gateway's ownership,example=10s,format=duration"`
}

func (oo *OxiaOptions) WithDefault() {
	if oo.ServiceAddress == "" {
		oo.ServiceAddress = "localhost:6648"
	}
	if oo.Namespace == "" {
		oo.Namespace = "default"
	}
	if oo.Cluster == "" {
		oo.Cluster = "default"
	}
	if oo.SessionTimeout == 0 {
		oo.SessionTimeout = 10 * time.Second
	}
}

func (oo *OxiaOptions) Validate() error {
	return errors.Join(
		notEmpty("oxia.serviceAddress", oo.ServiceAddress),
		notEmpty("oxia.namespace", oo.Namespace),
		notEmpty("oxia.cluster", oo.Cluster),
		positive("oxia.sessionTimeout", oo.SessionTimeout),
	)
}

type ObservabilityOptions struct {
	Metric MetricOptions `yaml:"metric" json:"metric" jsonschema:"description=Prometheus metrics and health endpoints"`
	Log    LogOptions    `yaml:"log" json:"log" jsonschema:"description=Logs"`
	Pprof  PprofOptions  `yaml:"pprof" json:"pprof" jsonschema:"description=Go profiles"`
}

func (ob *ObservabilityOptions) WithDefault() {
	ob.Metric.WithDefault()
	ob.Log.WithDefault()
	ob.Pprof.WithDefault()
}

func (ob *ObservabilityOptions) Validate() error {
	return errors.Join(
		ob.Metric.Validate(),
		ob.Log.Validate(),
		ob.Pprof.Validate(),
	)
}

type MetricOptions struct {
	// Enabled is a pointer so that a file without it keeps the metrics on
	Enabled *bool `yaml:"enabled" json:"enabled" jsonschema:"description=Serves the Prometheus metrics on /metrics,default=true"`
	// BindAddress is also the address of the endpoints that tools check,
	// which are served even with the metrics disabled
	BindAddress string `yaml:"bindAddress" json:"bindAddress" jsonschema:"description=Address of /metrics and of /health and /readyz and /livez and /version,example=0.0.0.0:8080,format=hostname"`
}

func (mo *MetricOptions) IsEnabled() bool {
	return mo.Enabled == nil || *mo.Enabled
}

func (mo *MetricOptions) WithDefault() {
	if mo.BindAddress == "" {
		mo.BindAddress = "0.0.0.0:8080"
	}
}

func (mo *MetricOptions) Validate() error {
	return notEmpty("observability.metric.bindAddress", mo.BindAddress)
}

type LogOptions struct {
	Level string `yaml:"level" json:"level" jsonschema:"description=Log level: debug or info or warn or error,default=info"`
}

// SlogLevel returns the level, or an error when it is not a level of log/slog.
func (lo *LogOptions) SlogLevel() (slog.Level, error) {
	var l slog.Level
	err := l.UnmarshalText([]byte(lo.Level))
	return l, err
}

func (lo *LogOptions) WithDefault() {
	if lo.Level == "" {
		lo.Level = "info"
	}
}

func (lo *LogOptions) Validate() error {
	if _, err := lo.SlogLevel(); err != nil {
		return fmt.Errorf("observability.log.level: %w", err)
	}
	return nil
}

type PprofOptions struct {
	// Enabled is a pointer so that a file without it keeps the profiles on
	Enabled     *bool  `yaml:"enabled" json:"enabled" jsonschema:"description=Serves the Go profiles on /debug/pprof,default=true"`
	BindAddress string `yaml:"bindAddress" json:"bindAddress" jsonschema:"description=Address of /debug/pprof,example=127.0.0.1:6060,format=hostname"`
}

func (po *PprofOptions) IsEnabled() bool {
	return po.Enabled == nil || *po.Enabled
}

func (po *PprofOptions) WithDefault() {
	if po.BindAddress == "" {
		po.BindAddress = "127.0.0.1:6060"
	}
}

func (po *PprofOptions) Validate() error {
	if !po.IsEnabled() {
		return nil
	}
	return notEmpty("observability.pprof.bindAddress", po.BindAddress)
}

func notEmpty(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s is empty", name)
	}
	return nil
}

func positive[T int | time.Duration](name string, value T) error {
	if value <= 0 {
		return fmt.Errorf("%s must be positive, not %v", name, value)
	}
	return nil
}
