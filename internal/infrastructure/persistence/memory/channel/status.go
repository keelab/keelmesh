package channel

import (
	"context"
	"slices"

	"github.com/keelab/keelith/ops"
)

// RuntimeStatus exposes one bounded channel lifecycle snapshot for Keelith Ops.
func (r *Repository) RuntimeStatus(context.Context) (ops.RuntimeStatus, error) {
	if r == nil || r.registry == nil {
		return ops.RuntimeStatus{State: "stopped", Ready: false}, nil
	}
	enabled := 0
	running := 0
	capabilitySet := make(map[string]struct{})
	inboundDropped := r.inboundDropped.Load()
	forwardDropped := r.forwardDropped.Load()
	forwardFailures := r.forwardFailures.Load()
	for _, item := range r.registry.All() {
		definition := item.Definition()
		if !definition.Enabled {
			continue
		}
		enabled++
		for _, capability := range definition.Capabilities {
			if capability != "" {
				capabilitySet[capability] = struct{}{}
			}
		}
		if item.Running() {
			running++
		}
	}

	state := "idle"
	ready := true
	degraded := false
	switch {
	case enabled > 0 && running == enabled:
		state = "running"
	case running > 0:
		state = "degraded"
		ready = false
		degraded = true
	case enabled > 0:
		state = "stopped"
		ready = false
	}

	capabilities := make([]string, 0, len(capabilitySet))
	for capability := range capabilitySet {
		capabilities = append(capabilities, capability)
	}
	slices.Sort(capabilities)
	return ops.RuntimeStatus{
		State:    state,
		Ready:    ready,
		Degraded: degraded,
		Active:   running,
		Counters: []ops.RuntimeCounter{
			{Name: "enabled", Value: uint64(enabled)},
			{Name: "running", Value: uint64(running)},
			{Name: "not_running", Value: uint64(enabled - running)},
			{Name: "inbound_dropped", Value: inboundDropped},
			{Name: "gate_forward_dropped", Value: forwardDropped},
			{Name: "gate_forward_failures", Value: forwardFailures},
		},
		Capabilities: capabilities,
	}, nil
}
