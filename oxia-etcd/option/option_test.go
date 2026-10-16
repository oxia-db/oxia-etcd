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

package option

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func defaults() Options {
	return Options{
		Server: ServerOptions{
			BindAddress:        "0.0.0.0:2379",
			AdvertiseClientURL: "http://127.0.0.1:2379",
			Name:               "oxia-etcd",
			HistorySize:        100_000,
			ProgressInterval:   5 * time.Second,
		},
		Oxia: OxiaOptions{
			ServiceAddress: "localhost:6648",
			Namespace:      "default",
			Cluster:        "default",
			SessionTimeout: 10 * time.Second,
		},
		Observability: ObservabilityOptions{
			Metric: MetricOptions{BindAddress: "0.0.0.0:8080"},
			Log:    LogOptions{Level: "info"},
			Pprof:  PprofOptions{BindAddress: "127.0.0.1:6060"},
		},
	}
}

func writeFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "oxia-etcd.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestDefaults(t *testing.T) {
	o := NewDefaultOptions()
	assert.Equal(t, defaults(), *o)
	assert.True(t, o.Observability.Metric.IsEnabled())
	assert.True(t, o.Observability.Pprof.IsEnabled())
	assert.NoError(t, o.Validate())

	loaded, err := Load("")
	require.NoError(t, err)
	assert.Equal(t, o, loaded)
}

// The example configuration sets every option to its default.
func TestLoadExample(t *testing.T) {
	o, err := Load("../../conf/oxia-etcd.yaml")
	require.NoError(t, err)
	assert.True(t, o.Observability.Metric.IsEnabled())
	assert.True(t, o.Observability.Pprof.IsEnabled())
	// Compare without the explicit enabled flags
	o.Observability.Metric.Enabled = nil
	o.Observability.Pprof.Enabled = nil
	assert.Equal(t, defaults(), *o)
}

// A file sets some options, and the others keep their defaults.
func TestLoadPartial(t *testing.T) {
	o, err := Load(writeFile(t, `
server:
  historySize: 10
  progressInterval: 1s
observability:
  metric:
    enabled: false
  log:
    level: DEBUG
`))
	require.NoError(t, err)
	want := defaults()
	want.Server.HistorySize = 10
	want.Server.ProgressInterval = time.Second
	want.Observability.Metric.Enabled = new(bool)
	want.Observability.Log.Level = "DEBUG"
	assert.Equal(t, want, *o)
	assert.False(t, o.Observability.Metric.IsEnabled())
	assert.True(t, o.Observability.Pprof.IsEnabled())
}

func TestLoadErrors(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	assert.ErrorIs(t, err, os.ErrNotExist)

	_, err = Load(writeFile(t, "server: ["))
	assert.Error(t, err)

	_, err = Load(writeFile(t, "observability:\n  log:\n    level: verbose\n"))
	assert.ErrorContains(t, err, "observability.log.level")
}

func TestValidate(t *testing.T) {
	for _, tc := range []struct {
		name   string
		modify func(*Options)
		err    string
	}{
		{"server bind address", func(o *Options) { o.Server.BindAddress = "" }, "server.bindAddress is empty"},
		{"advertise client URL", func(o *Options) { o.Server.AdvertiseClientURL = "" }, "server.advertiseClientURL is empty"},
		{"name", func(o *Options) { o.Server.Name = "" }, "server.name is empty"},
		{"history size", func(o *Options) { o.Server.HistorySize = -1 }, "server.historySize must be positive"},
		{"progress interval", func(o *Options) { o.Server.ProgressInterval = -time.Second }, "server.progressInterval must be positive"},
		{"service address", func(o *Options) { o.Oxia.ServiceAddress = "" }, "oxia.serviceAddress is empty"},
		{"namespace", func(o *Options) { o.Oxia.Namespace = "" }, "oxia.namespace is empty"},
		{"cluster", func(o *Options) { o.Oxia.Cluster = "" }, "oxia.cluster is empty"},
		{"session timeout", func(o *Options) { o.Oxia.SessionTimeout = 0 }, "oxia.sessionTimeout must be positive"},
		{"metric bind address", func(o *Options) { o.Observability.Metric.BindAddress = "" }, "observability.metric.bindAddress is empty"},
		{"log level", func(o *Options) { o.Observability.Log.Level = "verbose" }, "observability.log.level"},
		{"pprof bind address", func(o *Options) { o.Observability.Pprof.BindAddress = "" }, "observability.pprof.bindAddress is empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := NewDefaultOptions()
			tc.modify(o)
			assert.ErrorContains(t, o.Validate(), tc.err)
		})
	}

	// The metrics endpoint is served even with the metrics disabled, but not the profiles
	o := NewDefaultOptions()
	o.Observability.Metric.Enabled = new(bool)
	o.Observability.Metric.BindAddress = ""
	assert.Error(t, o.Validate())
	o = NewDefaultOptions()
	o.Observability.Pprof.Enabled = new(bool)
	o.Observability.Pprof.BindAddress = ""
	assert.NoError(t, o.Validate())

	// Every error is reported
	o = NewDefaultOptions()
	o.Server.Name = ""
	o.Oxia.Cluster = ""
	err := o.Validate()
	assert.ErrorContains(t, err, "server.name")
	assert.ErrorContains(t, err, "oxia.cluster")
}
