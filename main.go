package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

func or(channels ...<-chan any) <-chan any {
	switch len(channels) {
	case 0:
		return nil
	case 1:
		return channels[0]
	}
	stream := make(chan any)
	go func() {
		defer close(stream)
		switch len(channels) {
		case 2:
			select {
			case <-channels[0]:
			case <-channels[1]:
			}
		default:
			select {
			case <-channels[0]:
			case <-channels[1]:
			case <-channels[2]:
			case <-or(append(channels[3:], stream)...):
			}
		}
	}()

	return stream
}

func take(done <-chan any, stream <-chan any, nums int) <-chan any {
	newStream := make(chan any)
	go func() {
		defer close(newStream)
		for range nums {
			select {
			case <-done:
				return
			case newStream <- <-stream:
			}
		}
	}()
	return newStream
}

func main() {
	type startGoroutineFn func(done <-chan any, pulseInterval time.Duration) (hearbeat <-chan any)
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

	newSteward := func(
		timeout time.Duration,
		startGoroutine startGoroutineFn,
	) startGoroutineFn {
		return func(done <-chan any, pulseInterval time.Duration) <-chan any {
			heartbeat := make(chan any)
			go func() {
				defer close(heartbeat)
				var wardDone chan any
				var wardHeartbeat <-chan any
				startWard := func() {
					wardDone = make(chan any)
					wardHeartbeat = startGoroutine(or(wardDone, done), timeout/2)
				}
				startWard()
				pulse := time.Tick(pulseInterval)
			monitorLoop:
				for {
					timeoutSignal := time.After(timeout)
					for {
						select {
						case <-pulse:
							select {
							case heartbeat <- struct{}{}:
							default:
							}
						case <-wardHeartbeat:
							continue monitorLoop
						case <-timeoutSignal:
							log.Println("steward: ward unhealthy; restarting")
							close(wardDone)
							startWard()
							continue monitorLoop
						case <-done:
							return
						}
					}
				}
			}()
			return heartbeat
		}
	}
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ltime | log.LUTC)

	doWorkFn := func(done <-chan any, intList ...int) (startGoroutineFn, <-chan any) {
		intChanStream := make(chan (<-chan any))
		intStream := bridge(done, intChanStream)
		doWork := func(
			done <-chan any,
			pulseInterval time.Duration,
		) <-chan any {
			intStream := make(chan any)
			heartbeat := make(chan any)
			go func() {
				defer close(intStream)
				select {
				case intChanStream <- intStream:
				case <-done:
					return
				}
				pulse := time.Tick(pulseInterval)
				for {
				valueLoop:
					for _, intVal := range intList {
						if intVal < 0 {
							log.Printf("negative value: %v\n", intVal)
							return
						}
						for {
							select {
							case <-pulse:
								select {
								case heartbeat <- struct{}{}:
								default:
								}
							case intStream <- intVal:
								continue valueLoop
							case <-done:
								return
							}
						}
					}
				}
			}()
			return heartbeat
		}
		return doWork, intStream
	}

	done := make(chan any)
	defer close(done)
	doWork, intStream := doWorkFn(done, 1, 2, -1, 3, 4, 5)
	doWorkWithSteward := newSteward(1*time.Millisecond, doWork)
	doWorkWithSteward(done, 1*time.Hour)
	for intVal := range take(done, intStream, 6) {
		fmt.Printf("Received: %v\n", intVal)
	}
}
