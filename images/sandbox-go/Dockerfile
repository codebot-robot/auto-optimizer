# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Sandbox image for Go repositories.
#
# It bundles the geminicli-toolbox binary and the Go toolchain on top
# of a Debian base that provides build essentials for cgo and race detection.

FROM --platform=$BUILDPLATFORM golang:1.27.4-bookworm AS build
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -trimpath -buildvcs=false -ldflags="-s -w" \
      -o /geminicli-toolbox ./cmd/geminicli-toolbox

FROM golang:1.27.4-bookworm AS go-dist

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git curl procps tini make gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /geminicli-toolbox /usr/local/bin/geminicli-toolbox
COPY --from=go-dist /usr/local/go /usr/local/go

RUN useradd -u 1000 -m -d /home/auto-optimizer -s /bin/bash auto-optimizer

ENV HOME=/home/auto-optimizer \
    GOPATH=/home/auto-optimizer/go \
    GOCACHE=/home/auto-optimizer/.cache/go-build \
    GOMODCACHE=/home/auto-optimizer/go/pkg/mod \
    PATH=/usr/local/go/bin:/home/auto-optimizer/go/bin:${PATH}

USER 1000
WORKDIR /home/auto-optimizer

CMD ["sleep", "infinity"]
