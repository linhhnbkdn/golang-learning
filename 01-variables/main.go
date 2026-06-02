package main

import "fmt"

func main() {
	// Khai báo tường minh với var
	var name string = "Li"
	var age int = 25

	// Khai báo ngắn gọn với := (chỉ dùng trong function)
	height := 1.65

	// Hằng số
	const Pi = 3.14159

	// Zero values — giá trị mặc định khi không khởi tạo
	var x int    // 0
	var s string // ""
	var b bool   // false

	fmt.Println(name, age, height, Pi)
	fmt.Println("zero values:", x, s, b)

	// Khai báo nhiều biến cùng lúc
	var (
		city       = "Hanoi"
		population = 8_000_000
	)
	fmt.Println(city, population)

	// TODO: Khai báo biến country (string), area (float64), isTropical (bool)
	// Gán giá trị và in ra: "Vietnam has area 331212.0 km2, tropical: true"
	var country string = "Vietnam"
	var area float64 = 331212.0
	var isTropical bool = true

	fmt.Printf("%s has area %.1f km2, tropical: %t\n", country, area, isTropical)
}
