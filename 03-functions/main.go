package main

import "fmt"

// Hàm cơ bản
func add(a int, b int) int {
	return a + b
}

// Shorthand khi cùng kiểu
func multiply(a, b int) int {
	return a * b
}

// Trả về nhiều giá trị — đặc trưng của Go
func divide(a, b float64) (float64, error) {
	if b == 0 {
		return 0, fmt.Errorf("cannot divide by zero")
	}
	return a / b, nil
}

// Variadic — nhận số lượng tham số không giới hạn
func sum(nums ...int) int {
	total := 0
	for _, n := range nums {
		total += n
	}
	return total
}

func minMax(nums ...int) (int, int) {
	if len(nums) == 0 {
		panic("minMax requires at least one number")
	}
	min, max := nums[0], nums[0]
	for _, n := range nums {
		if n < min {
			min = n
		}
		if n > max {
			max = n
		}
	}
	return min, max
}

func main() {
	fmt.Println("3+5 =", add(3, 5))
	fmt.Println("3*5 =", multiply(3, 5))

	result, err := divide(10, 3)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Printf("10/3 = %.2f\n", result)
	}

	_, err = divide(5, 0)
	fmt.Println("5/0 error:", err)

	fmt.Println("sum(1..5) =", sum(1, 2, 3, 4, 5))

	// Spread slice vào variadic
	nums := []int{10, 20, 30}
	fmt.Println("sum from slice =", sum(nums...))

	// TODO: Viết hàm minMax(nums ...int) (int, int)
	// Trả về (min, max) trong danh sách
	// Test: minMax(3, 1, 4, 1, 5, 9, 2, 6) → min=1, max=9
	min, max := minMax(3, 1, 4, 1, 5, 9, 2, 6)
	fmt.Printf("min=%d, max=%d\n", min, max)
}
