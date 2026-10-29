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

// Package state defines the filesystem records and layout under the state directory.
//
// Layout under the state directory:
//
//	repos/<host>/<owner>/<name>/repo.json
//	repos/<host>/<owner>/<name>/goals/<goal>/goal.json
//	repos/<host>/<owner>/<name>/runs/<run-id>/run.json
//	repos/<host>/<owner>/<name>/runs/<run-id>/events.jsonl
//
// Single-writer concurrency model:
// Each file under the state directory has exactly one writer:
//   - The syncer owns repo.json and goal.json.
//   - The run orchestrator owns run.json and events.jsonl.
//
// Write semantics:
// JSON files are written atomically by writing to a temporary file in the same
// directory, fsyncing, and renaming to the target path.
// events.jsonl is append-only with one JSON object per line.
package state
