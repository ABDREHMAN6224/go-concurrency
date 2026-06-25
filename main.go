package main

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// sends nil channel in doWork which now remains in memory till process exits
func GoRoutineLeak() {
	doWork := func(strings <-chan string) <-chan any {
		compeleted := make(chan any)
		go func() {
			defer fmt.Println("do work ended")
			defer close(compeleted)
			// it will be waiting here forever
			for s := range strings {
				fmt.Println(s)
			}
		}()
		return compeleted
	}
	doWork(nil)
	fmt.Println("Done")
}

func GoRoutineLeakFixed() {
	doWork := func(done <-chan any, strings <-chan string) <-chan any {
		compeleted := make(chan any)
		go func() {
			defer fmt.Println("doWork ended")
			defer close(compeleted)
			// it will be waiting here forever
			for {
				select {
				case s := <-strings:
					fmt.Println(s)
				case <-done:
					return
				}
			}
		}()
		return compeleted
	}
	done := make(chan any)
	terminated := doWork(done, nil)
	go func() {
		// Cancel the operation after 1 second.
		time.Sleep(1 * time.Second)
		fmt.Println("Canceling doWork goroutine...")
		close(done)
	}()
	<-terminated
	fmt.Println("Done.")
}

func exmaple2Bad() {
	newRandStream := func(done <-chan any) <-chan int {
		randStream := make(chan int)
		go func() {
			defer fmt.Println("newRandStream closure exited.")
			defer close(randStream)
			for {
				select {
				case <-done:
					return
				case randStream <- rand.Int():
				}
			}
		}()
		return randStream
	}
	done := make(chan any)
	randStream := newRandStream(done)
	fmt.Println("3 random ints:")
	for i := 1; i <= 3; i++ {
		fmt.Printf("%d: %d\n", i, <-randStream)
	}
	close(done)
	time.Sleep(time.Second)
}

func main() {
	// GoRoutineLeak()
	// GoRoutineLeakFixed()
	exmaple2Bad()
}
