package main

import (
	"fmt"
	"math/rand/v2"
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

func take(done <-chan any, valueStream <-chan any, nums int) <-chan any {
	takeStream := make(chan any)
	go func() {
		defer close(takeStream)
		for range nums {
			select {
			case <-done:
				return
			case takeStream <- <-valueStream:
			}
		}
	}()
	return takeStream
}

func takeRepeatFunc() {
	repeat := func(done <-chan any, fn func() any) <-chan any {
		stream := make(chan any)
		go func() {
			defer close(stream)
			for {
				select {
				case stream <- fn():
				case <-done:
					return

				}
			}
		}()
		return stream
	}
	done := make(chan any)
	defer close(done)
	rand := func() any { return rand.Int() }
	for num := range take(done, repeat(done, rand), 10) {
		fmt.Println(num)
	}
}

func main() {
	orDone := func(done, c <-chan any) <-chan any {
		valStream := make(chan any)
		go func() {
			defer close(valStream)
			for {
				select {
				case <-done:
					return
				case valStream <- c:
				}
			}
		}()
		return valStream
	}
}
