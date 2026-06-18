package main

import "testing"

func BenchmarkNextID(b *testing.B) {
	idGen := IDGenerator{
		machineId: 7,
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = idGen.NextID()
		}
	})
}
