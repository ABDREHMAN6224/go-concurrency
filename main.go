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

func repeat(done <-chan any, values ...int) <-chan any {
	stream := make(chan any)
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

func main() {
	orDone := func(done <-chan any, c <-chan any) <-chan any {
		valStream := make(chan any)
		go func() {
			defer close(valStream)
			for {
				select {
				case <-done:
					return
				case v, ok := <-c:
					if ok == false {
						return
					}
					select {
					case valStream <- v:
					case <-done:
					}
				}
			}
		}()
		return valStream
	}
	tee := func(
		done <-chan any,
		in <-chan any,
	) (_, _ <-chan any) {
		out1 := make(chan any)
		out2 := make(chan any)
		go func() {
			defer close(out1)
			defer close(out2)
			for val := range orDone(done, in) {
				var out1, out2 = out1, out2
				for i := 0; i < 2; i++ {
					select {
					case <-done:
					case out1 <- val:
						out1 = nil
					case out2 <- val:
						out2 = nil
					}
				}
			}
		}()
		return out1, out2
	}
	done := make(chan any)
	defer close(done)
	out1, out2 := tee(done, take(done, repeat(done, 1, 2), 4))
	for val1 := range out1 {
		fmt.Printf("out1: %v, out2: %v\n", val1, <-out2)
	}
}
