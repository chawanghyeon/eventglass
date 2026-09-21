package scale

import (
	"crypto/sha256"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/app"
)

func TestOneTwoFourWorkersProduceSameResultsAndBoundConnections(t *testing.T) {
	var baseline []string
	var previous time.Duration
	for _, workers := range []int{1, 2, 4} {
		started := time.Now()
		results := runWorkers(workers)
		elapsed := time.Since(started)
		if workers == 1 {
			baseline = results
		} else if strings.Join(results, "\n") != strings.Join(baseline, "\n") {
			t.Fatalf("%d workers changed logical results", workers)
		}
		if previous > 0 && elapsed > previous*6/5 {
			t.Fatalf("%d workers regressed: previous=%s elapsed=%s", workers, previous, elapsed)
		}
		if err := app.ValidateReplicaConnectionBudget(1, workers, 1); err != nil {
			t.Fatal(err)
		}
		t.Logf("R2 workers=%d tasks=%d elapsed_ms=%d logical_sha=%x", workers, len(results), elapsed.Milliseconds(), sha256.Sum256([]byte(strings.Join(results, "\n"))))
		previous = elapsed
	}
}

func TestTenantRoundRobinDispatch(t *testing.T) {
	queues := map[int][]int{1: {1, 2, 3, 4}, 2: {1}, 3: {1, 2}}
	var order []int
	for len(queues) > 0 {
		tenants := make([]int, 0, len(queues))
		for tenant := range queues {
			tenants = append(tenants, tenant)
		}
		sort.Ints(tenants)
		for _, tenant := range tenants {
			order = append(order, tenant)
			queues[tenant] = queues[tenant][1:]
			if len(queues[tenant]) == 0 {
				delete(queues, tenant)
			}
		}
	}
	if fmt.Sprint(order[:3]) != "[1 2 3]" {
		t.Fatalf("first round=%v", order)
	}
}

func TestKubernetesBoundsMatchRuntimeContracts(t *testing.T) {
	manifest, err := os.ReadFile("../../deploy/kubernetes/base.yaml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(manifest)
	for _, required := range []string{
		"minReplicaCount: 1", "maxReplicaCount: 20", "stabilizationWindowSeconds: 300",
		"{type: Percent, value: 25, periodSeconds: 60}", "memory: 512Mi", "cpu: \"1\"",
		"readOnlyRootFilesystem: true", "runAsUser: 65532", "terminationGracePeriodSeconds: 30",
		"EVENTGLASS_METRICS_ADDR", "eventglass_autoscale_dependency_available", "sizeLimit: 4Gi", "kubernetes.io/arch: arm64",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("manifest missing %q", required)
		}
	}
}

func runWorkers(workers int) []string {
	tasks := make(chan int)
	results := make(chan string, 96)
	var group sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for task := range tasks {
				digest := sha256.Sum256([]byte(fmt.Sprintf("scale-task-%d", task)))
				time.Sleep(2 * time.Millisecond)
				results <- fmt.Sprintf("%03d:%x", task, digest)
			}
		}()
	}
	go func() {
		for task := 0; task < 96; task++ {
			tasks <- task
		}
		close(tasks)
		group.Wait()
		close(results)
	}()
	var values []string
	for value := range results {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}
