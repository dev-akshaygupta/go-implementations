package main

import (
	"sort"
	"sync"
	"sync/atomic"
)

type PlayerScore struct {
	Player string
	Score  int
}

type Leaderboard struct {
	mu         sync.RWMutex
	scores     map[string]int
	totalReads atomic.Int64
}

type LeaderboardPlainMutex struct {
	mu         sync.Mutex
	scores     map[string]int
	totalReads atomic.Int64
}

func NewLeaderboard() *Leaderboard {
	return &Leaderboard{
		scores: map[string]int{},
	}
}

func NewLeaderboardPlainMutex() *Leaderboard {
	return &Leaderboard{
		scores: map[string]int{},
	}
}

func (lb *Leaderboard) UpdateScore(player string, score int) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	lb.scores[player] = score
}

func (lb *Leaderboard) GetTopN(n int) []PlayerScore {
	lb.mu.RLock()

	lb.totalReads.Add(1)

	playerScore := make([]PlayerScore, 0, len(lb.scores))
	for player, score := range lb.scores {
		playerScore = append(playerScore, PlayerScore{
			Player: player, Score: score,
		})
	}

	lb.mu.RUnlock()

	sort.Slice(playerScore, func(i, j int) bool {
		return playerScore[i].Score > playerScore[j].Score
	})

	if n > len(playerScore) {
		return playerScore
	}

	return playerScore[:n]
}

func (ln *Leaderboard) TotalReads() int64 {
	return ln.totalReads.Load()
}
