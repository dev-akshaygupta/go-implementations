package main

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

func main() {
	leaderBoard := NewLeaderboard()
	players := []string{
		"amit",
		"sumit",
		"ronit",
		"rohit",
		"mohit",
		"sunny",
		"honey",
		"ronny",
		"jatin",
		"mani",
	}

	// Seed 10 players with random scores
	for _, player := range players {
		leaderBoard.UpdateScore(player, rand.IntN(1001))
	}

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			for range 20 {
				topLeader := leaderBoard.GetTopN(3)
				for _, ps := range topLeader {
					fmt.Println(ps.Player, ps.Score)
				}
				fmt.Println("-----------------------")
				time.Sleep(200 * time.Millisecond)
			}
		})
	}

	wg.Go(func() {
		for range 50 {
			player := players[rand.IntN(len(players))]
			score := rand.IntN(1001)

			leaderBoard.UpdateScore(player, score)
			fmt.Printf("[Writer] Updated %-6s -> %d\n", player, score)

			time.Sleep(100 * time.Millisecond)
		}
	})

	wg.Wait()

	fmt.Println("\n========== Simulation Complete ==========")

	fmt.Printf("Total Reads Served: %d\n\n", leaderBoard.totalReads.Load())

	fmt.Println("Final Top 3 Players:")

	top3 := leaderBoard.GetTopN(3)

	for rank, player := range top3 {
		fmt.Printf(
			"%d. %-6s %d\n",
			rank+1,
			player.Player,
			player.Score,
		)
	}
}
