package main

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

func BenchmarkGetTopNRWMutex(b *testing.B) {
	lb := NewLeaderboard()

	for i := 0; i < 1000; i++ {
		lb.UpdateScore(fmt.Sprintf("player-%d", i), rand.IntN(10000))
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = lb.GetTopN(10)
		}
	})
}

func BenchmarkGetTopNPlainMutex(b *testing.B) {
	lb := NewLeaderboardPlainMutex()

	for i := 0; i < 1000; i++ {
		lb.UpdateScorePlainMutex(fmt.Sprintf("player-%d", i), rand.IntN(10000))
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = lb.GetTopNPlainMutex(10)
		}
	})
}
