package main

import (
	"fmt"
	"sync"
	"time"
)

func worker(id int, wg *sync.WaitGroup) {
	defer wg.Done() // báo WaitGroup khi xong
	fmt.Printf("worker %d starting\n", id)
	time.Sleep(50 * time.Millisecond) // giả lập công việc
	fmt.Printf("worker %d done\n", id)
}

func main() {
	// Goroutine cơ bản — go keyword
	go fmt.Println("hello from goroutine")
	time.Sleep(10 * time.Millisecond) // đợi goroutine in xong

	fmt.Println("---")

	// WaitGroup — chờ nhiều goroutine
	var wg sync.WaitGroup

	for i := 1; i <= 5; i++ {
		wg.Add(1)         // tăng counter trước khi spawn
		go worker(i, &wg) // truyền &wg — pointer để share
	}

	wg.Wait() // block cho đến khi counter về 0
	fmt.Println("all workers done")

	fmt.Println("---")

	// Mutex — bảo vệ shared data khỏi race condition
	var mu sync.Mutex
	counter := 0

	for i := 0; i < 100000000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mu.Lock()
			counter++ // critical section
			fmt.Println(counter)
			mu.Unlock()
		}()
	}
	wg.Wait()
	fmt.Println("counter (no race):", counter) // luôn = 1000

	// TODO: Tạo 5 goroutines, mỗi goroutine nhận index i (1-5)
	// Tính bình phương và in: "square of 3 = 9"
	// Dùng WaitGroup để chờ tất cả xong
	// Lưu ý: capture biến loop đúng cách (truyền vào tham số, không capture trực tiếp)
}
