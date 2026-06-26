package main

import "fmt"

func main() {
	orDone := func(done <-chan any, stream <-chan any) <-chan any {
		newStream := make(chan any)
		go func() {
			defer close(newStream)
			for {
				select {
				case <-done:
					return
				case v, ok := <-stream:
					if ok == false {
						return
					}
					select {
					case <-done:
						return
					case newStream <- v:
					}
				}
			}
		}()
		return newStream
	}
	bridge := func(done <-chan any, chanStream <-chan <-chan any) <-chan any {
		valStream := make(chan any)
		go func() {
			defer close(valStream)
			for {
				var stream <-chan any
				select {
				case <-done:
					return
				case maybeStream, ok := <-chanStream:
					if ok == false {
						return
					}
					stream = maybeStream
				}
				for val := range orDone(done, stream) {
					select {
					case valStream <- val:
					case <-done:
						return

					}
				}
			}
		}()
		return valStream
	}

	genVals := func() <-chan <-chan any {
		chanStream := make(chan (<-chan any))
		go func() {
			defer close(chanStream)
			for i := range 10 {
				stream := make(chan any, 1)
				stream <- i
				close(stream)
				chanStream <- stream
			}
		}()
		return chanStream
	}
	for v := range bridge(nil, genVals()) {
		fmt.Printf("%v ", v)
	}
}
