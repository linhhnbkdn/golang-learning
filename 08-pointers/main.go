package main

import "fmt"

// Pointer receiver cho phép mutate struct
type Counter struct {
	value int
}

func (c *Counter) Increment() { c.value++ }
func (c Counter) Value() int  { return c.value }

// Truyền pointer để sửa biến ngoài
func double(n *int) {
	*n *= 2
}

func swap(a, b *int) {
	*a, *b = *b, *a
}

func main() {
	x := 42
	p := &x // p là *int — pointer tới x

	fmt.Println("x  =", x)
	fmt.Println("&x =", &x) // địa chỉ ô nhớ
	fmt.Println("p  =", p)
	fmt.Println("*p =", *p) // dereference — lấy giá trị tại địa chỉ

	*p = 100 // sửa x qua pointer
	fmt.Println("x after *p=100:", x)

	double(&x)
	fmt.Println("x after double:", x)

	a, b := 1, 2
	swap(&a, &b)
	fmt.Println("after swap:", a, b)

	// new() — tạo pointer với zero value
	q := new(int) // *int trỏ tới 0
	fmt.Println("new int:", *q)

	// Pointer to struct
	c := &Counter{}
	c.Increment()
	c.Increment()
	c.Increment()
	fmt.Println("counter:", c.Value())

	// Nil pointer
	var np *int
	fmt.Println("nil pointer:", np)
	// *np  ← panic! đừng dereference nil

	// TODO: Viết hàm resetToZero(n *int) — set giá trị về 0
	// Viết hàm addAndStore(a, b int, result *int) — cộng a+b, lưu vào result
	// Test cả hai
}
