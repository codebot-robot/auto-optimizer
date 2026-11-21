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

package session

import (
	"context"
	"errors"
	"fmt"
	"sync"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
)

// SandboxOwner manages a single active Sandbox resource that can be shared across
// one or more sessions. It holds the active Sandbox state, knows how to create
// and prepare one, and replaces a dead Sandbox under a mutex lock.
//
// The lock is held while a Sandbox is created, awaited and prepared, which can take
// minutes. That is acceptable, because every session of the run needs the Sandbox.
type SandboxOwner struct {
	mu sync.Mutex

	harness         *Harness
	activeSandbox   *Sandbox
	sandboxOpts     *CreateSandboxOptions
	prepareSandbox  func(ctx context.Context, sb *Sandbox) error
	restoreSandbox  func(ctx context.Context, sb *Sandbox) error
	extendLifecycle bool
}

// NewSandboxOwner creates a SandboxOwner with the given harness.
func NewSandboxOwner(h *Harness) *SandboxOwner {
	return &SandboxOwner{
		harness:         h,
		extendLifecycle: true,
	}
}

// ActiveSandbox returns the current active sandbox, or nil if none is provisioned.
func (o *SandboxOwner) ActiveSandbox() *Sandbox {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.activeSandbox
}

// SetActiveSandbox binds an existing sandbox to this owner.
func (o *SandboxOwner) SetActiveSandbox(sb *Sandbox) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.activeSandbox = sb
}

// SetCreateSandboxOptions configures the options used when creating a sandbox on demand.
func (o *SandboxOwner) SetCreateSandboxOptions(opts CreateSandboxOptions) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.sandboxOpts = &opts
	o.extendLifecycle = opts.ExtendLifecycle
}

// SetSandboxPreparer configures the preparation step executed on a ready sandbox
// before any tool call runs in it.
func (o *SandboxOwner) SetSandboxPreparer(fn func(ctx context.Context, sb *Sandbox) error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.prepareSandbox = fn
}

// SetSandboxRestorer configures the restoration step executed on a ready sandbox
// when no preparer is configured (e.g. restoring filesystem for standalone sessions).
func (o *SandboxOwner) SetSandboxRestorer(fn func(ctx context.Context, sb *Sandbox) error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.restoreSandbox = fn
}

// ExtendLifecycle extends the sandbox shutdown time if lifecycle extension is enabled.
func (o *SandboxOwner) ExtendLifecycle(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.extendLifecycle || o.activeSandbox == nil {
		return nil
	}
	return o.activeSandbox.ExtendLifecycle(ctx, SandboxInactivityTimeout)
}

// ExtendLifecycleEnabled reports whether sandbox lifecycle extension is enabled.
func (o *SandboxOwner) ExtendLifecycleEnabled() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.extendLifecycle
}

// HasWorkspaceClaim reports whether the sandbox options name a workspace claim.
func (o *SandboxOwner) HasWorkspaceClaim() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.hasWorkspaceClaimLocked()
}

func (o *SandboxOwner) hasWorkspaceClaimLocked() bool {
	return o.sandboxOpts != nil && o.sandboxOpts.WorkspaceClaimName != ""
}

// EnsureSandbox returns the running sandbox, creating and preparing it if it does not
// yet exist or was cleaned up due to inactivity.
func (o *SandboxOwner) EnsureSandbox(ctx context.Context) (tools.Sandbox, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.ensureSandboxLocked(ctx)
}

// ReplaceDead drops the dead sandbox and replaces it under lock with a newly created
// and prepared sandbox. If another concurrent caller has already replaced the dead sandbox,
// the existing replacement is returned without creating another.
// If the given sandbox is not dead and has not been replaced, it returns (dead, false, nil).
func (o *SandboxOwner) ReplaceDead(ctx context.Context, dead tools.Sandbox) (tools.Sandbox, bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	deadSb, _ := dead.(*Sandbox)

	// If another concurrent caller has already replaced dead with a new live sandbox,
	// return that replacement.
	if deadSb != nil && o.activeSandbox != nil && o.activeSandbox != deadSb && !isSandboxDead(ctx, o.activeSandbox) {
		return o.activeSandbox, true, nil
	}

	// If dead is not dead in Kubernetes, do not replace it (was an ordinary tool error).
	if deadSb != nil && !isSandboxDead(ctx, deadSb) {
		return dead, false, nil
	}

	o.dropDeadSandboxLocked(ctx, dead)
	newSb, err := o.ensureSandboxLocked(ctx)
	if err != nil {
		return nil, false, err
	}
	return newSb, true, nil
}

// DropDeadSandbox closes and releases a sandbox that has died or expired.
func (o *SandboxOwner) DropDeadSandbox(ctx context.Context, dead tools.Sandbox) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.dropDeadSandboxLocked(ctx, dead)
}

func (o *SandboxOwner) dropDeadSandboxLocked(ctx context.Context, dead tools.Sandbox) {
	target, _ := dead.(*Sandbox)
	if target == nil {
		target = o.activeSandbox
	}
	if target == nil {
		return
	}
	log := scoped.Log(ctx)
	if err := target.Close(); err != nil {
		log.V(2).Info("error closing dead sandbox connection", "error", err)
	}

	client := target.client
	if o.harness != nil && o.harness.sandboxClient != nil {
		client = o.harness.sandboxClient
	}
	if client != nil {
		if err := client.DeleteSandboxByName(context.WithoutCancel(ctx), target.id.Namespace, target.id.Name); err != nil && !k8serrors.IsNotFound(err) {
			log.Error(err, "failed to delete dead sandbox", "sandbox", target.id.Name)
		}
		client.ReleaseSandbox(target)
	}
	if o.activeSandbox == target {
		o.activeSandbox = nil
	}
}

func (o *SandboxOwner) createSandbox(ctx context.Context) (*Sandbox, error) {
	if o.harness == nil {
		return nil, errors.New("harness is required to create a sandbox")
	}

	opts := CreateSandboxOptions{ExtendLifecycle: o.extendLifecycle}
	if o.sandboxOpts != nil {
		opts = *o.sandboxOpts
	}
	if opts.Name == "" {
		return nil, errors.New("cannot create sandbox without name in options")
	}

	id := types.NamespacedName{
		Namespace: o.harness.opts.Namespace,
		Name:      opts.Name,
	}

	return o.harness.createSandboxResourceWithMetadata(ctx, id, opts)
}

func (o *SandboxOwner) ensureSandboxLocked(ctx context.Context) (tools.Sandbox, error) {
	if o.harness != nil && o.harness.sandboxOverride != nil {
		return o.harness.sandboxOverride, nil
	}

	log := scoped.Log(ctx)

	if sb := o.activeSandbox; sb != nil {
		if o.extendLifecycle {
			if err := sb.ExtendLifecycle(ctx, SandboxInactivityTimeout); err != nil {
				if k8serrors.IsNotFound(err) {
					log.Info("Active sandbox was deleted or expired, will recreate it", "sandbox.name", sb.SandboxName())
					o.dropDeadSandboxLocked(ctx, sb)
				} else {
					return nil, fmt.Errorf("extending sandbox TTL: %w", err)
				}
			}
		}
	}

	if o.activeSandbox == nil {
		log.Info("launching sandbox for tool execution...")

		sb, err := o.createSandbox(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create sandbox: %w", err)
		}

		if err := sb.WaitForReady(ctx); err != nil {
			log.Error(err, "sandbox not ready")
			if o.harness != nil && o.harness.sandboxClient != nil {
				if delErr := o.harness.sandboxClient.DeleteSandbox(context.WithoutCancel(ctx), sb); delErr != nil {
					log.Error(delErr, "failed to delete unready sandbox")
				}
			}
			return nil, err
		}

		log.V(1).Info("sandbox ready", "sandbox.name", sb.SandboxName())

		if o.prepareSandbox != nil {
			log.Info("preparing sandbox...", "sandbox.name", sb.SandboxName())
			if err := o.prepareSandbox(ctx, sb); err != nil {
				log.Error(err, "failed to prepare sandbox")
				if o.harness != nil && o.harness.sandboxClient != nil {
					if delErr := o.harness.sandboxClient.DeleteSandbox(context.WithoutCancel(ctx), sb); delErr != nil {
						log.Error(delErr, "failed to delete unprepared sandbox")
					}
				}
				return nil, err
			}
		} else if !o.hasWorkspaceClaimLocked() && o.restoreSandbox != nil {
			log.Info("restoring filesystem to sandbox...", "sandbox.name", sb.SandboxName())
			if err := o.restoreSandbox(ctx, sb); err != nil {
				log.Error(err, "failed to restore filesystem; starting with a fresh sandbox instead", "sandbox.name", sb.SandboxName())
			}
		}

		o.activeSandbox = sb
	}

	return o.activeSandbox, nil
}
