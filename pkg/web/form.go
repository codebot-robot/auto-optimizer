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
	"crypto/hmac"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/gke-labs/auto-optimizer/pkg/web/auth"
)

const (
	csrfFormField        = "csrf"
	csrfForbiddenMessage = "Forbidden: CSRF token missing or invalid"
)

var (
	errBadToken      = errors.New("bad anti-forgery token")
	errMalformedForm = errors.New("invalid form data")
)

func (s *Server) writeFormError(w http.ResponseWriter, err error) {
	if errors.Is(err, errBadToken) {
		http.Error(w, csrfForbiddenMessage, http.StatusForbidden)
		return
	}
	http.Error(w, err.Error(), http.StatusBadRequest)
}

func (s *Server) validateCSRF(r *http.Request) error {
	if err := r.ParseForm(); err != nil {
		return fmt.Errorf("%w: %v", errMalformedForm, err)
	}
	var formToken string
	if vals := r.PostForm[csrfFormField]; len(vals) > 0 {
		formToken = vals[0]
	}
	if formToken == "" {
		return errBadToken
	}
	if s.requireSignIn {
		cookie, err := r.Cookie(auth.CookieName)
		if err != nil || cookie.Value == "" {
			return errBadToken
		}
		expected := auth.CSRFToken(cookie.Value, s.sessionKey)
		if expected == "" || !hmac.Equal([]byte(formToken), []byte(expected)) {
			return errBadToken
		}
		return nil
	}
	cookie, err := r.Cookie(csrfCookieName)
	if err != nil || cookie.Value == "" || !hmac.Equal([]byte(formToken), []byte(cookie.Value)) {
		return errBadToken
	}
	return nil
}

// parseForm parses a URL-encoded form body into a typed struct dst and verifies the
// anti-forgery token in the same call.
//
// It reads only the POST request body (r.PostForm) and not the URL query parameters.
// Struct fields must be strings and must declare their form field with a `form:"..."` tag.
// Surrounding whitespace is trimmed from all string fields. Any unknown form field is rejected.
// If dst is not a non-nil pointer to a struct, or if a struct field is not a string, parseForm panics.
//
// parseForm returns errBadToken if the anti-forgery token is missing or invalid, or
// errMalformedForm if the request body cannot be parsed or contains unknown fields.
func (s *Server) parseForm(r *http.Request, dst any) error {
	val := reflect.ValueOf(dst)
	if !val.IsValid() || val.Kind() != reflect.Pointer || val.IsNil() || val.Elem().Kind() != reflect.Struct {
		panic("web: dst must be a non-nil pointer to a struct")
	}
	elem := val.Elem()

	if err := s.validateCSRF(r); err != nil {
		return err
	}

	typ := elem.Type()
	fields := make(map[string]reflect.Value, elem.NumField())
	for i := 0; i < elem.NumField(); i++ {
		f := typ.Field(i)
		if !f.IsExported() {
			continue
		}
		tag := f.Tag.Get("form")
		if tag == "" || tag == "-" {
			continue
		}
		fieldVal := elem.Field(i)
		if fieldVal.Kind() != reflect.String {
			panic(fmt.Sprintf("web: unsupported field type %s for %s", fieldVal.Kind(), f.Name))
		}
		fields[tag] = fieldVal
	}

	for key, values := range r.PostForm {
		if key == csrfFormField {
			continue
		}
		fieldVal, ok := fields[key]
		if !ok {
			return fmt.Errorf("%w: unknown field %q", errMalformedForm, key)
		}
		if len(values) > 0 {
			fieldVal.SetString(strings.TrimSpace(values[0]))
		}
	}
	return nil
}
