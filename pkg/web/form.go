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

package web

import (
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
)

var (
	// ErrBadToken is returned when the anti-forgery token is missing or invalid.
	ErrBadToken = errors.New("bad anti-forgery token")

	// ErrMalformedForm is returned when the form body is malformed or contains invalid/unknown fields.
	ErrMalformedForm = errors.New("invalid form data")
)

// parseForm parses a URL-encoded form into a typed struct dst and verifies the anti-forgery token.
func (s *Server) parseForm(r *http.Request, dst any) error {
	val := reflect.ValueOf(dst)
	if !val.IsValid() || val.Kind() != reflect.Pointer || val.IsNil() || val.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("%w: destination must be a pointer to a struct", ErrMalformedForm)
	}
	elem := val.Elem()

	if err := r.ParseForm(); err != nil {
		return fmt.Errorf("%w: %v", ErrMalformedForm, err)
	}
	if !s.validateCSRF(r) {
		return ErrBadToken
	}

	typ := elem.Type()
	fields := make(map[string]reflect.Value, elem.NumField())
	for i := 0; i < elem.NumField(); i++ {
		f := typ.Field(i)
		if !f.IsExported() {
			continue
		}
		tag := strings.Split(f.Tag.Get("form"), ",")[0]
		if tag == "" || tag == "-" {
			continue
		}
		fieldVal := elem.Field(i)
		if fieldVal.Kind() != reflect.String {
			return fmt.Errorf("%w: unsupported field type %s for %s", ErrMalformedForm, fieldVal.Kind(), f.Name)
		}
		fields[tag] = fieldVal
	}

	for key, values := range r.PostForm {
		if key == "csrf" {
			continue
		}
		fieldVal, ok := fields[key]
		if !ok {
			return fmt.Errorf("%w: unknown field %q", ErrMalformedForm, key)
		}
		if len(values) > 0 {
			fieldVal.SetString(strings.TrimSpace(values[0]))
		}
	}
	return nil
}
