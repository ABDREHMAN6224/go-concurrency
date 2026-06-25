package main

import (
	"fmt"
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

func main() {
	// GoRoutineLeak()
	GoRoutineLeakFixed()
}
