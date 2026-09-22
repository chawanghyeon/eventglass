package app

import "testing"

func TestCompactionManagedMemoryIsBoundedSeparately(t *testing.T) {
	for _, test := range []struct {
		name                  string
		requested, role, want int64
	}{
		{"worker", 256 << 20, 256 << 20, 96 << 20},
		{"combined", 256 << 20, 192 << 20, 96 << 20},
		{"smaller_role", 256 << 20, 32 << 20, 32 << 20},
		{"smaller_request", 32 << 20, 192 << 20, 32 << 20},
		{"default_request", 0, 256 << 20, 96 << 20},
	} {
		t.Run(test.name, func(t *testing.T) {
			gate := NewNativeTaskGate()
			gate.memory = test.role
			runner := ProcessCompactionRunner{Gate: gate}
			if got := runner.memoryLimit(test.requested); got != test.want {
				t.Fatalf("managed memory=%d want=%d", got, test.want)
			}
		})
	}
	if got := (ProcessCompactionRunner{}).memoryLimit(256 << 20); got != 96<<20 {
		t.Fatalf("explicit standalone runner escaped maintenance limit: %d", got)
	}
}
