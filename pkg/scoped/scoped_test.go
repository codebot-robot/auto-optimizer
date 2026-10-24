// Copyright 2026 Google LLC
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

package scoped

import (
	"testing"

	"github.com/go-logr/logr"
)

func TestLog(t *testing.T) {
	// Default when unset
	def := Log(t.Context())
	// Default logger must be usable and not panic on Info
	def.V(1).Info("test default log")

	// Round trip when set
	custom := logr.Discard()
	ctx := WithLog(t.Context(), custom)
	if got := Log(ctx); got != custom {
		t.Errorf("Log(ctx) = %v, want %v", got, custom)
	}
}
