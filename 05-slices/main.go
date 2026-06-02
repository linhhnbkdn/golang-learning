package main

import "fmt"

func reverse(s []int) []int {
	n := len(s)
	reversed := make([]int, n)
	for i, v := range s {
		reversed[n-1-i] = v
	}
	return reversed
}

func main() {
	// Array — fixed size, ít dùng trực tiếp
	arr := [3]int{1, 2, 3}
	fmt.Println("array:", arr)

	// Slice — dynamic, thường dùng hơn
	s := []int{1, 2, 3, 4, 5}
	fmt.Println("slice:", s, "len:", len(s), "cap:", cap(s))

	// append
	s = append(s, 6, 7)
	fmt.Println("after append:", s)

	// Slicing [low:high] — không bao gồm high
	fmt.Println("s[1:4] =", s[1:4]) // index 1,2,3
	fmt.Println("s[:3]  =", s[:3])
	fmt.Println("s[5:]  =", s[5:])

	// make([]T, len, cap)
	zeros := make([]int, 3, 5)
	fmt.Println("make:", zeros, "len:", len(zeros), "cap:", cap(zeros))

	// copy
	dst := make([]int, len(s))
	n := copy(dst, s)
	fmt.Println("copied", n, "elements:", dst)

	// 2D slice
	matrix := [][]int{
		{1, 2, 3},
		{4, 5, 6},
		{7, 8, 9},
	}
	for _, row := range matrix {
		fmt.Println(row)
		fmt.Println("row len:", len(row), "cap:", cap(row))
	}

	// TODO: Viết hàm reverse(s []int) []int
	// Đảo ngược slice, trả về slice MỚI (không sửa bản gốc)
	// Test: reverse([]int{1,2,3,4,5}) → [5 4 3 2 1]
	// Hint: dùng make để tạo slice mới
	fmt.Println("reversed:", reverse([]int{1, 2, 3, 4, 5}))
}
