package main

import (
	"sync"
	"sync/atomic"
	"time"
)

type IDGenerator struct {
	mu         sync.Mutex
	machineId  int64
	sequence   atomic.Int64
	lastMillis atomic.Int64
}

func NewIDGenerator(machineId int64) *IDGenerator {
	return &IDGenerator{
		machineId: machineId,
	}
}

func waitNextMillis(last int64) int64 {
	for {
		now := time.Now().UnixMilli()
		if now > last {
			return now
		}
	}
}

func (ig *IDGenerator) NextID() int64 {
	for {
		ig.mu.Lock()
		now := time.Now().UnixMilli()

		last := ig.lastMillis.Load()

		if now > last {
			ig.lastMillis.Store(now)
			ig.sequence.Store(0)
		}

		seq := ig.sequence.Add(1)

		if seq > 4096 {
			ig.mu.Unlock()

			waitNextMillis(now)
			continue
		}

		ig.mu.Unlock()
		return (now << 22) | (ig.machineId << 12) | (seq & 0xFFF)
	}
}

func (ig *IDGenerator) Decode(id int64) (millis int64, machineId int64, sequence int64) {
	sequence = id & 0xFFF
	machineId = (id >> 12) & 0x3FF
	millis = id >> 22

	return millis, machineId, sequence
}
