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

# This image builds sandboxd from a pinned commit on our fork (github.com/justinsb/agent-sandbox)
# to carry changes required by auto-optimizer before they land upstream.
# It will be removed once an upstream release includes these changes.

ARG SANDBOXD_REPO=https://github.com/justinsb/agent-sandbox
ARG SANDBOXD_COMMIT=76156ce957a58d6d82e1cdd60e75d5be0d80a94f

FROM --platform=$BUILDPLATFORM golang:1.27.3 AS builder
ARG TARGETARCH
ARG SANDBOXD_REPO
ARG SANDBOXD_COMMIT

WORKDIR /src

RUN git init . \
    && git remote add origin "${SANDBOXD_REPO}" \
    && git fetch --depth=1 origin "${SANDBOXD_COMMIT}" \
    && git checkout FETCH_HEAD

RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -trimpath \
    -ldflags="-s -w \
        -X sigs.k8s.io/agent-sandbox/internal/version.gitVersion=${SANDBOXD_COMMIT} \
        -X sigs.k8s.io/agent-sandbox/internal/version.gitSHA=${SANDBOXD_COMMIT}" \
    -o /sandboxd ./packages/sandboxd/cmd/sandboxd

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates procps \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /sandboxd /usr/local/bin/sandboxd

RUN mkdir -p /workspace && chown 1000:1000 /workspace
WORKDIR /workspace
USER 1000

EXPOSE 8080 9090

ENTRYPOINT ["/usr/local/bin/sandboxd"]
