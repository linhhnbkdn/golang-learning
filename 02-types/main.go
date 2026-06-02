package main

import "fmt"

func main() {
	// Basic types
	var i int = 42
	var f float64 = 3.14
	var s string = "hello"
	var b bool = true

	// In ra kiểu dữ liệu với %T
	fmt.Printf("i=%v  type=%T\n", i, i)
	fmt.Printf("f=%v  type=%T\n", f, f)
	fmt.Printf("s=%v  type=%T\n", s, s)
	fmt.Printf("b=%v  type=%T\n", b, b)

	// Type conversion — phải explicit, Go KHÔNG tự cast
	f2 := float64(i)
	i2 := int(f) // truncate, không round
	fmt.Printf("float64(42)=%v  int(3.14)=%v\n", f2, i2)

	// String ↔ rune/byte
	r := 'A'             // rune = int32
	fmt.Printf("rune A = %d, char = %c\n", r, r)
	fmt.Printf("string A = %s\n", string(r))

	// TODO: Khai báo tuổi (int=22), GPA (float64=3.75), tên (string="Li")
	// Convert tuổi → float64, convert GPA → int
	// In ra tất cả kèm kiểu dữ liệu dùng %T
}
