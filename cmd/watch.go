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

package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

// watchConfig calls apply with the configuration at path each time the file
// changes. It watches the directory, so that it follows the file when it is
// replaced, as Kubernetes does with the files of a ConfigMap.
func watchConfig(ctx context.Context, log *slog.Logger, path string, apply func(Config)) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	if err := w.Add(filepath.Dir(path)); err != nil {
		_ = w.Close()
		return err
	}
	last, _ := os.ReadFile(path)
	go func() {
		defer w.Close()
		// Changes come in bursts: reload once they settle
		var settle <-chan time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-w.Errors:
				log.Warn("Failed to watch the configuration", slog.Any("error", err))
			case <-w.Events:
				settle = time.After(100 * time.Millisecond)
			case <-settle:
				data, err := os.ReadFile(path)
				if err != nil || bytes.Equal(data, last) {
					continue
				}
				last = data
				cfg, err := loadConfig(path)
				if err != nil {
					log.Warn("Ignoring the invalid configuration", slog.String("path", path), slog.Any("error", err))
					continue
				}
				log.Info("Reloaded the configuration", slog.String("path", path))
				apply(cfg)
			}
		}
	}()
	return nil
}
