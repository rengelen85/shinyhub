package proxy

import (
	"testing"

	"github.com/rvben/shinyhub/internal/config"
)

func TestDrainingReplicaRetainedWorkIsPerWorker(t *testing.T) {
	p := New()
	p.SetPoolSize("app", 2)
	idle := &replicaBackend{index: 0}
	busy := &replicaBackend{index: 1}
	busy.activeConns.Store(1)
	p.pools["app"].drainingGenerations = map[int64][]*replicaBackend{10: {idle, busy}}
	p.pools["app"].generationPolicies = map[int64]GenerationPolicy{10: {Mode: config.IsolationMultiplex}}
	if p.DrainingReplicaHasRetainedWork("app", 10, 0) || !p.DrainingReplicaHasRetainedWork("app", 10, 1) {
		t.Fatal("one busy predecessor kept the idle predecessor charged")
	}
	busy.activeConns.Store(0)
	busy.assignedClients = 1
	p.pools["app"].generationPolicies[10] = GenerationPolicy{Mode: config.IsolationGrouped}
	if p.DrainingReplicaHasRetainedWork("app", 10, 0) || !p.DrainingReplicaHasRetainedWork("app", 10, 1) {
		t.Fatal("grouped bindings were not charged to their own worker")
	}
	delete(p.pools["app"].drainingGenerations, 10)
	if p.DrainingReplicaHasRetainedWork("app", 10, 1) {
		t.Fatal("retired route retained CPU accounting")
	}
}
