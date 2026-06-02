package main

import (
	"fmt"
	"time"
)

func generator(n int) <-chan int { // chan chỉ đọc
	ch := make(chan int)
	go func() {
		for i := 1; i <= n; i++ {
			ch <- i
		}
		close(ch) // báo consumer không còn giá trị nào nữa
	}()
	return ch
}

func square(in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		for v := range in {
			out <- v * v
		}
		close(out)
	}()
	return out
}

func main() {
	// Unbuffered channel — send block cho đến khi có receiver
	ch := make(chan int)
	go func() { ch <- 42 }()
	fmt.Println("received:", <-ch)

	// Buffered channel — send không block cho đến khi đầy
	buf := make(chan string, 3)
	buf <- "a"
	buf <- "b"
	buf <- "c"
	fmt.Println(<-buf, <-buf, <-buf)

	// Range over channel
	nums := generator(5)
	for v := range nums {
		fmt.Print(v, " ")
	}
	fmt.Println()

	// Pipeline pattern
	squares := square(generator(5))
	for v := range squares {
		fmt.Print(v, " ")
	}
	fmt.Println()

	// select — chọn channel sẵn sàng trước
	ch1 := make(chan string)
	ch2 := make(chan string)
	go func() { time.Sleep(50 * time.Millisecond); ch1 <- "slow" }()
	go func() { time.Sleep(20 * time.Millisecond); ch2 <- "fast" }()

	for i := 0; i < 2; i++ {
		select {
		case msg := <-ch1:
			fmt.Println("ch1:", msg)
		case msg := <-ch2:
			fmt.Println("ch2:", msg)
		}
	}

	// TODO: Viết pipeline 3 bước:
	// gen(n) → gửi 1..n
	// double(in) → nhân đôi mỗi giá trị
	// filter(in) → chỉ giữ số chẵn
	// main nhận và in ra tất cả
	// Test với n=10
}
