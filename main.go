package main

import "time"

func or(channels ...<-chan any) <-chan any {
	switch len(channels) {
	case 0:
		return nil
	case 1:
		return channels[0]
	}
	stream := make(chan any)

	return stream
}

func main() {
	type startGoroutineFn func(done <-chan any, pulseInterval time.Duration) (hearbeat <-chan any)

	newSteward := func(
		timeout time.Duration,
		startGoroutine startGoroutineFn,
	) startGoroutineFn {
		return func(done <-chan any, pulseInterval time.Duration) <-chan any {
			heartbeat := make(chan any)
			go func() {
				defer close(heartbeat)
				var wardDone chan interface{}
				var wardHeartbeat <-chan interface{}
				startWard := func() {
					wardDone = make(chan interface{})
					wardHeartbeat = startGoroutine(or(wardDone, done), timeout/2)
				}
				startWard()
				pulse := time.Tick(pulseInterval)
			}()
			return heartbeat
		}
	}
}
