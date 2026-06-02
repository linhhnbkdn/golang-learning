package main

import "fmt"

func main() {
	// for — Go chỉ có for, không có while/do-while
	for i := 0; i < 5; i++ {
		fmt.Print(i, " ")
	}
	fmt.Println()

	// while-style
	n := 1
	for n < 100 {
		n *= 2
	}
	fmt.Println("first power of 2 >= 100:", n)

	// range over slice
	fruits := []string{"apple", "banana", "cherry"}
	for i, f := range fruits {
		fmt.Printf("%d: %s\n", i, f)
	}

	// range — bỏ index
	for _, f := range fruits {
		fmt.Print(f, " ")
	}
	fmt.Println()

	// if/else — không cần ()
	score := 75
	if score >= 90 {
		fmt.Println("A")
	} else if score >= 70 {
		fmt.Println("B")
	} else {
		fmt.Println("C")
	}

	// if với init statement
	if x := score * 2; x > 100 {
		fmt.Println("x > 100:", x)
	}

	// switch — không cần break, tự break
	day := "Monday"
	switch day {
	case "Saturday", "Sunday":
		fmt.Println("Weekend")
	case "Monday":
		fmt.Println("Start of week")
	default:
		fmt.Println("Weekday")
	}

	// switch không có condition (như if-else chain)
	switch {
	case score >= 90:
		fmt.Println("Excellent")
	case score >= 70:
		fmt.Println("Good")
	default:
		fmt.Println("Average")
	}

	// TODO: Viết FizzBuzz từ 1 đến 30
	// Chia hết 15 → "FizzBuzz", chia hết 3 → "Fizz", chia hết 5 → "Buzz"
	// Còn lại → in số đó
}
