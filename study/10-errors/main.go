package main

import (
	"errors"
	"fmt"
)

// Custom error type — implement error interface (Error() string)
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation: %s — %s", e.Field, e.Message)
}

// Sentinel error — so sánh bằng errors.Is
var ErrDivByZero = errors.New("division by zero")

func divide(a, b float64) (float64, error) {
	if b == 0 {
		return 0, ErrDivByZero
	}
	return a / b, nil
}

func validateAge(age int) error {
	if age < 0 {
		return &ValidationError{Field: "age", Message: "must be non-negative"}
	}
	if age > 150 {
		return &ValidationError{Field: "age", Message: "unrealistic value"}
	}
	return nil
}

func loadUser(id int) error {
	err := validateAge(-5)
	if err != nil {
		// Wrap error với %w — giữ nguyên error gốc
		return fmt.Errorf("loadUser(id=%d): %w", id, err)
	}
	return nil
}

func main() {
	// Basic error handling pattern trong Go
	result, err := divide(10, 3)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Printf("10/3 = %.2f\n", result)
	}

	// errors.Is — so sánh kể cả khi wrapped
	_, err = divide(5, 0)
	if errors.Is(err, ErrDivByZero) {
		fmt.Println("caught sentinel error:", err)
	}

	// errors.As — unwrap về custom type
	err = loadUser(42)
	var ve *ValidationError
	if errors.As(err, &ve) {
		fmt.Printf("field=%s msg=%s\n", ve.Field, ve.Message)
	}
	fmt.Println("full wrapped error:", err)

	// TODO: Viết hàm sqrt(n float64) (float64, error)
	// Định nghĩa NegativeInputError{Value float64} implement error
	// sqrt(-4) → trả về NegativeInputError
	// sqrt(9)  → trả về 3.0, nil
	// Dùng errors.As để unwrap và in ra giá trị âm
}
