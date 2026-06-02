package main

import "fmt"

// Closure — function capture biến từ scope bên ngoài
func makeCounter() func() int {
	count := 0 // biến này "sống" theo closure
	return func() int {
		count++
		return count
	}
}

func makeAdder(x int) func(int) int {
	return func(y int) int {
		return x + y // x được capture
	}
}

// Higher-order functions
func mapInts(nums []int, f func(int) int) []int {
	result := make([]int, len(nums))
	for i, n := range nums {
		result[i] = f(n)
	}
	return result
}

func filter(nums []int, pred func(int) bool) []int {
	var result []int
	for _, n := range nums {
		if pred(n) {
			result = append(result, n)
		}
	}
	return result
}

func reduce(nums []int, init int, f func(int, int) int) int {
	acc := init
	for _, n := range nums {
		acc = f(acc, n)
	}
	return acc
}

func main() {
	// Counter — mỗi closure có state riêng
	c1 := makeCounter()
	c2 := makeCounter()
	fmt.Println(c1(), c1(), c1()) // 1 2 3
	fmt.Println(c2(), c2())       // 1 2  (độc lập với c1)

	// Adder
	add5 := makeAdder(5)
	add10 := makeAdder(10)
	fmt.Println(add5(3), add5(7))   // 8 12
	fmt.Println(add10(3), add10(7)) // 13 17

	nums := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	doubled := mapInts(nums, func(n int) int { return n * 2 })
	fmt.Println("doubled:", doubled)

	evens := filter(nums, func(n int) bool { return n%2 == 0 })
	fmt.Println("evens:", evens)

	sum := reduce(nums, 0, func(a, b int) int { return a + b })
	fmt.Println("sum:", sum)

	// Closure capture — cẩn thận với vòng lặp
	funcs := make([]func(), 3)
	for i := 0; i < 3; i++ {
		i := i // shadow — tạo bản copy mới mỗi iteration
		funcs[i] = func() { fmt.Print(i, " ") }
	}
	for _, f := range funcs {
		f()
	}
	fmt.Println()

	// TODO: Viết hàm memoize(f func(int) int) func(int) int
	// Wrap một function, cache kết quả đã tính để tránh tính lại
	// Test: memoize fibonacci — đếm số lần thực sự tính
}
