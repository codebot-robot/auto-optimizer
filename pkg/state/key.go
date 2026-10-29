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
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	validGoalNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]*$`)
)

// ValidateRepoKey checks that all components of RepoKey are non-empty and safe for use in paths.
func ValidateRepoKey(key RepoKey) error {
	if key.Host == "" {
		return errors.New("repository host cannot be empty")
	}
	if key.Owner == "" {
		return errors.New("repository owner cannot be empty")
	}
	if key.Name == "" {
		return errors.New("repository name cannot be empty")
	}

	for field, val := range map[string]string{
		"host":  key.Host,
		"owner": key.Owner,
		"name":  key.Name,
	} {
		if strings.Contains(val, "/") || strings.Contains(val, "\\") || strings.Contains(val, "\x00") {
			return fmt.Errorf("repository %s %q contains invalid characters", field, val)
		}
		if val == "." || val == ".." || strings.Contains(val, "..") {
			return fmt.Errorf("repository %s %q contains illegal relative path component", field, val)
		}
	}

	return nil
}

// ValidateGoalName checks that the goal name is non-empty and safe in branch names and paths.
func ValidateGoalName(name string) error {
	if name == "" {
		return errors.New("goal name cannot be empty")
	}
	if !validGoalNamePattern.MatchString(name) {
		return fmt.Errorf("invalid goal name %q: must contain only lowercase letters, digits, '-', '_', or '.'", name)
	}
	if name == "." || name == ".." || strings.Contains(name, "..") {
		return fmt.Errorf("goal name %q contains illegal relative path component", name)
	}
	return nil
}

// ParseRepoURL parses a raw git or HTTP URL into a RepoKey.
// It maps both remote URLs (e.g. github.com/gke-labs/x) and local paths safely.
func ParseRepoURL(rawURL string) (RepoKey, error) {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return RepoKey{}, errors.New("repository URL cannot be empty")
	}

	// Case 1: scp-like syntax, e.g. git@github.com:owner/name.git
	if strings.Contains(trimmed, "@") && strings.Contains(trimmed, ":") && !strings.Contains(trimmed, "://") {
		atIdx := strings.Index(trimmed, "@")
		colonIdx := strings.Index(trimmed, ":")
		if colonIdx > atIdx {
			host := trimmed[atIdx+1 : colonIdx]
			path := strings.TrimPrefix(trimmed[colonIdx+1:], "/")
			path = strings.TrimSuffix(path, ".git")
			parts := strings.Split(path, "/")
			if len(parts) >= 2 {
				owner := parts[len(parts)-2]
				name := parts[len(parts)-1]
				key := RepoKey{Host: host, Owner: owner, Name: name}
				if err := ValidateRepoKey(key); err != nil {
					return RepoKey{}, err
				}
				return key, nil
			}
		}
	}

	// Case 2: URL with scheme (http, https, ssh, git, file)
	if strings.Contains(trimmed, "://") {
		u, err := url.Parse(trimmed)
		if err != nil {
			return RepoKey{}, fmt.Errorf("parsing URL %q: %w", trimmed, err)
		}

		if u.Scheme == "file" {
			cleanPath := filepath.Clean(u.Path)
			cleanPath = strings.TrimSuffix(cleanPath, ".git")
			name := filepath.Base(cleanPath)
			dir := filepath.Dir(cleanPath)
			owner := filepath.Base(dir)
			if owner == "/" || owner == "." || owner == "" {
				owner = "_"
			}
			key := RepoKey{Host: "local", Owner: owner, Name: name}
			if err := ValidateRepoKey(key); err != nil {
				return RepoKey{}, err
			}
			return key, nil
		}

		host := u.Hostname()
		if host == "" {
			host = u.Host
		}
		path := strings.Trim(u.Path, "/")
		path = strings.TrimSuffix(path, ".git")
		parts := strings.Split(path, "/")
		if len(parts) >= 2 {
			owner := parts[len(parts)-2]
			name := parts[len(parts)-1]
			key := RepoKey{Host: host, Owner: owner, Name: name}
			if err := ValidateRepoKey(key); err != nil {
				return RepoKey{}, err
			}
			return key, nil
		}
		if len(parts) == 1 && parts[0] != "" {
			key := RepoKey{Host: host, Owner: "_", Name: parts[0]}
			if err := ValidateRepoKey(key); err != nil {
				return RepoKey{}, err
			}
			return key, nil
		}
	}

	// Case 3: remote host/owner/name shorthand (e.g. github.com/owner/name)
	parts := strings.Split(trimmed, "/")
	if len(parts) == 3 && strings.Contains(parts[0], ".") && !strings.HasPrefix(parts[0], ".") {
		name := strings.TrimSuffix(parts[2], ".git")
		key := RepoKey{Host: parts[0], Owner: parts[1], Name: name}
		if err := ValidateRepoKey(key); err != nil {
			return RepoKey{}, err
		}
		return key, nil
	}

	// Case 4: local filesystem path (/path/to/repo or ./repo)
	if strings.HasPrefix(trimmed, "/") || strings.HasPrefix(trimmed, ".") || strings.Contains(trimmed, string(filepath.Separator)) {
		cleanPath := filepath.Clean(trimmed)
		cleanPath = strings.TrimSuffix(cleanPath, ".git")
		name := filepath.Base(cleanPath)
		dir := filepath.Dir(cleanPath)
		owner := filepath.Base(dir)
		if owner == "/" || owner == "." || owner == "" {
			owner = "_"
		}
		key := RepoKey{Host: "local", Owner: owner, Name: name}
		if err := ValidateRepoKey(key); err != nil {
			return RepoKey{}, err
		}
		return key, nil
	}

	// Case 5: bare repo name
	name := strings.TrimSuffix(trimmed, ".git")
	key := RepoKey{Host: "local", Owner: "_", Name: name}
	if err := ValidateRepoKey(key); err != nil {
		return RepoKey{}, err
	}
	return key, nil
}
