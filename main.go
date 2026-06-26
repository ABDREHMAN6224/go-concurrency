package main

import (
	"fmt"
	"time"
)

func addMulPipelien() {
	generator := func(done <-chan any, integers ...int) <-chan int {
		intStream := make(chan int)
		go func() {
			defer close(intStream)
			for _, v := range integers {
				select {
				case <-done:
					return
				case intStream <- v:
				}
			}
		}()
		return intStream
	}
	multiply := func(done <-chan any, intStream <-chan int, multiplier int) <-chan int {
		mStream := make(chan int)
		go func() {
			defer close(mStream)
			for v := range intStream {
				select {
				case <-done:
					return
				case mStream <- v * multiplier:
				}
			}
		}()
		return mStream
	}
	add := func(done <-chan any, intStream <-chan int, additive int) <-chan int {
		aStream := make(chan int)
		go func() {
			defer close(aStream)
			for v := range intStream {
				select {
				case <-done:
					return
				case aStream <- v + additive:
				}
			}
		}()
		return aStream
	}
	done := make(chan any)
	defer close(done)
	intStream := generator(done, 1, 2, 3, 4)
	pipeline := multiply(done, add(done, multiply(done, intStream, 2), 1), 2)
	for v := range pipeline {
		fmt.Println(v)
	}
}

func repeatValues() {

	repeat := func(done <-chan any, values ...int) <-chan int {
		stream := make(chan int)
		go func() {
			defer close(stream)
			for {
				for _, v := range values {
					select {
					case <-done:
						return
					case stream <- v:
					}
				}
			}
		}()
		return stream
	}
	done := make(chan any)
	go func() {
		defer close(done)
		time.Sleep(1 * time.Second)
	}()
	for i := range repeat(done, 1, 2, 3, 4, 5) {
		fmt.Println(i)
	}
}

func main() {
	// addMulPipelien()
	// repeatValues()
}
