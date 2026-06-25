package main

import "fmt"

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

func main() {
	GoRoutineLeak()
}
