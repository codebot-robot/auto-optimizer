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

package state

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"time"
)

const (
	// runIDTimeFormat is the UTC ISO8601-like timestamp format used in run IDs.
	runIDTimeFormat = "20060102T150405Z"
)

var runIDPattern = regexp.MustCompile(`^[0-9]{8}T[0-9]{6}Z-[0-9a-fA-F]{4}$`)

// NewRunID generates a new time-sortable run ID at current UTC time.
// Example: "20261023T120000Z-ab12".
func NewRunID() (string, error) {
	return NewRunIDAt(time.Now().UTC())
}

// NewRunIDAt generates a new time-sortable run ID with the given timestamp.
func NewRunIDAt(t time.Time) (string, error) {
	var suffix [2]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", fmt.Errorf("generating random suffix for run id: %w", err)
	}
	return fmt.Sprintf("%s-%s", t.UTC().Format(runIDTimeFormat), hex.EncodeToString(suffix[:])), nil
}

// ValidateRunID checks if a run ID matches the expected format.
func ValidateRunID(id string) error {
	if id == "" {
		return errors.New("run id cannot be empty")
	}
	if !runIDPattern.MatchString(id) {
		return fmt.Errorf("invalid run id %q: must match format %s-<hex4>", id, runIDTimeFormat)
	}
	return nil
}

// ParseRunIDTime parses the UTC timestamp from a run ID.
func ParseRunIDTime(id string) (time.Time, error) {
	if err := ValidateRunID(id); err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse(runIDTimeFormat, id[:16])
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing run id timestamp from %q: %w", id, err)
	}
	return t, nil
}
