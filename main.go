package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func main() {
	doWork := func(done <-chan any, id int, result chan<- int) {
		started := time.Now()
		loadTime := time.Duration(1+rand.Intn(5)) * time.Second
		select {
		case <-done:
		case <-time.After(loadTime):
		}
		select {
		case <-done:
		case result <- id:
		}
		took := time.Since(started)
		if took < loadTime {
			took = loadTime
		}
		fmt.Printf("%v took %v\n", id, took)
	}
	done := make(chan any)
	result := make(chan int)
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Go(func() { doWork(done, i, result) })
	}
	firstReturned := <-result
	close(done)
	wg.Wait()
	fmt.Printf("Received an answer from #%v\n", firstReturned)
}
