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

package commands

import (
	"context"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/klog/v2"
)

// DefaultServerAddress returns the default gRPC server address.
func DefaultServerAddress() string {
	if s := os.Getenv("AO_SERVER"); s != "" {
		return s
	}
	return "127.0.0.1:8081"
}

// dialGRPC connects to the specified gRPC server address.
func dialGRPC(serverAddr string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connecting to server %s: %w", serverAddr, err)
	}
	return conn, nil
}

// closeGRPCConn closes a gRPC client connection and logs any error at V(4).
func closeGRPCConn(ctx context.Context, conn *grpc.ClientConn) {
	if err := conn.Close(); err != nil {
		klog.FromContext(ctx).V(4).Info("closing gRPC connection", "error", err)
	}
}
