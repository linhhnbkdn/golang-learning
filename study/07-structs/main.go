package main

import "fmt"

// Struct definition
type Person struct {
	Name  string
	Age   int
	Email string
}

// Value receiver — không thay đổi được gốc
func (p Person) Greet() string {
	return fmt.Sprintf("Hi, I'm %s, age %d", p.Name, p.Age)
}

// Pointer receiver — thay đổi được gốc, dùng khi cần mutate
func (p *Person) Birthday() {
	p.Age++
}

// Constructor function (Go không có constructor, tự viết)
func NewPerson(name string, age int, email string) *Person {
	return &Person{Name: name, Age: age, Email: email}
}

// Embedding — composition thay vì inheritance
type Employee struct {
	Person  // embedded, không phải field thông thường
	Company string
	Salary  float64
}

type Rectangle struct {
	Width  float64
	Height float64
}

func (r Rectangle) Area() float64 {
	return r.Width * r.Height
}

func (r Rectangle) Perimeter() float64 {
	return 2 * (r.Width + r.Height)
}

func NewRectangle(width, height float64) *Rectangle {
	return &Rectangle{Width: width, Height: height}
}

func main() {
	// Struct literal
	p1 := Person{Name: "Alice", Age: 30, Email: "alice@example.com"}
	fmt.Println(p1.Greet())

	// Constructor
	p2 := NewPerson("Bob", 25, "bob@example.com")
	p2.Birthday()
	fmt.Println(p2.Greet())

	// Anonymous struct
	point := struct{ X, Y int }{X: 10, Y: 20}
	fmt.Println("point:", point)

	// Embedding
	e := Employee{
		Person:  Person{Name: "Carol", Age: 28, Email: "carol@co.com"},
		Company: "Acme",
		Salary:  75_000,
	}
	fmt.Println(e.Name, "at", e.Company) // promote field từ Person
	fmt.Println(e.Greet())               // promote method từ Person

	// TODO: Tạo struct Rectangle với Width, Height float64
	// Thêm method Area() float64 và Perimeter() float64
	// Constructor NewRectangle(width, height float64) *Rectangle
	// Test: 5x3 → Area=15, Perimeter=16
	rect := NewRectangle(5, 3)
	fmt.Printf("Rectangle: width=%.2f, height=%.2f\n", rect.Width, rect.Height)
	fmt.Printf("Area: %.2f\n", rect.Area())
	fmt.Printf("Perimeter: %.2f\n", rect.Perimeter())
}
