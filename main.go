package main

import (
	"bytes"
	"fmt"
	"sync"
)

// 1. Confinement - information is only available to one concurrent process (ad hoc and lexical)

func addHocConfinemeent() {
	data := make([]int, 4)
	loopData := func(handleData chan<- int) {
		defer close(handleData)
		for i := range data {
			handleData <- i
		}
	}
	handleData := make(chan int)
	go loopData(handleData)
	for num := range handleData {
		fmt.Println(num)
	}
}

func lexicalConfiement() {
	chanOwner := func() <-chan int {
		results := make(chan int, 5)
		go func() {
			defer close(results)
			for i := range 5 {
				results <- i
			}
		}()
		return results
	}
	consumer := func(results <-chan int) {
		for result := range results {
			fmt.Printf("Recieved: %d\n", result)
		}
		fmt.Print("Done Recieving")
	}
	results := chanOwner()
	consumer(results)
}

func withoutConfinement() {
	printData := func(wg *sync.WaitGroup, data []byte) {
		defer wg.Done()
		var buff bytes.Buffer
		for _, b := range data {
			fmt.Fprintf(&buff, "%c", b)
		}
		fmt.Println(buff.String())
	}

	var wg sync.WaitGroup
	wg.Add(2)
	data := []byte("golang")
	go printData(&wg, data[:3])
	go printData(&wg, data[3:])
	wg.Wait()
}

func main() {
	// addHocConfinemeent()
	// lexicalConfiement()
}
