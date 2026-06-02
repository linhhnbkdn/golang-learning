package main

import "fmt"

func fileOperation() {
	fmt.Println("1. opening file")
	defer fmt.Println("4. closing file") // defer = chạy khi hàm kết thúc, LIFO
	defer fmt.Println("3. flushing buffer")

	fmt.Println("2. writing data")
	// return sớm cũng sẽ chạy defers
}

func deferInLoop() {
	fmt.Println("defer LIFO order:")
	for i := 0; i < 3; i++ {
		defer fmt.Print(i, " ") // deferred: 2 1 0 (LIFO)
	}
}

// recover phải ở trong deferred function
func safeDiv(a, b int) (result int, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recovered: %v", r)
		}
	}()

	result = a / b // panic nếu b=0
	return
}

func mustPositive(n int) int {
	if n <= 0 {
		panic(fmt.Sprintf("expected positive, got %d", n))
	}
	return n
}

func safePositive(n int) (result int, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	result = mustPositive(n)
	return
}

func main() {
	// defer — LIFO, chạy kể cả khi có return sớm hay panic
	fileOperation()
	fmt.Println()

	deferInLoop()
	fmt.Println()

	// panic & recover
	r1, err := safeDiv(10, 2)
	fmt.Printf("10/2 = %d, err = %v\n", r1, err)

	r2, err := safeDiv(10, 0)
	fmt.Printf("10/0 = %d, err = %v\n", r2, err)

	r3, err := safePositive(-5)
	fmt.Printf("mustPositive(-5) = %d, err = %v\n", r3, err)

	// TODO: Viết hàm readConfig(path string) (string, error)
	// Dùng defer để in "done reading <path>" khi hàm kết thúc
	// Nếu path == "" thì panic("empty path")
	// Dùng recover để bắt panic và trả về error thay vì crash
	// Test: readConfig("app.yaml") và readConfig("")
}
