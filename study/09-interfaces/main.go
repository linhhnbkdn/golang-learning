package main

import (
	"fmt"
	"math"
)

// Interface — tập hợp method signatures
// Go dùng duck typing: struct nào có đủ methods thì implement interface đó
type Shape interface {
	Area() float64
	Perimeter() float64
}

type Circle struct {
	Radius float64
}

func (c Circle) Area() float64      { return math.Pi * c.Radius * c.Radius }
func (c Circle) Perimeter() float64 { return 2 * math.Pi * c.Radius }

type Rectangle struct {
	Width, Height float64
}

func (r Rectangle) Area() float64      { return r.Width * r.Height }
func (r Rectangle) Perimeter() float64 { return 2 * (r.Width + r.Height) }

// Function nhận interface — polymorphism
func printInfo(s Shape) {
	fmt.Printf("Area=%.2f  Perimeter=%.2f\n", s.Area(), s.Perimeter())
}

// Stringer interface từ fmt — implement để custom print
type Point struct{ X, Y float64 }

func (p Point) String() string {
	return fmt.Sprintf("(%.1f, %.1f)", p.X, p.Y)
}

func main() {
	shapes := []Shape{
		Circle{Radius: 5},
		Rectangle{Width: 4, Height: 3},
	}

	for _, s := range shapes {
		printInfo(s)

		// Type switch — kiểm tra kiểu thực tế
		switch v := s.(type) {
		case Circle:
			fmt.Printf("  Circle  radius=%.1f\n", v.Radius)
		case Rectangle:
			fmt.Printf("  Rectangle %gx%g\n", v.Width, v.Height)
		}
	}

	// Type assertion
	var s Shape = Circle{Radius: 3}
	c, ok := s.(Circle)
	fmt.Printf("is Circle: %v, radius: %.1f\n", ok, c.Radius)

	// Stringer
	p := Point{1.5, 2.5}
	fmt.Println(p) // gọi String() tự động

	// TODO: Tạo struct Triangle với 3 cạnh A, B, C float64
	// Implement Shape: Perimeter = A+B+C, Area dùng Heron's formula:
	//   s = (A+B+C)/2;  area = sqrt(s*(s-A)*(s-B)*(s-C))
	// Thêm vào shapes và test
}
