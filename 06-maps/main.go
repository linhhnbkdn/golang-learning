package main

import (
	"fmt"
	"strings"
)

func main() {
	// Tạo map với literal
	scores := map[string]int{
		"alice": 90,
		"bob":   85,
		"carol": 92,
	}

	// Đọc
	fmt.Println("alice:", scores["alice"])

	// Đọc an toàn — kiểm tra tồn tại
	score, ok := scores["dave"]
	fmt.Printf("dave: %d, exists: %v\n", score, ok) // 0, false

	// Thêm / cập nhật
	scores["dave"] = 88
	scores["alice"] = 95

	// Xoá
	delete(scores, "bob")

	// Duyệt — thứ tự NGẪU NHIÊN, không đảm bảo
	for name, s := range scores {
		fmt.Printf("  %s: %d\n", name, s)
	}

	// make
	counts := make(map[string]int)
	words := []string{"go", "python", "go", "java", "go", "python"}
	for _, w := range words {
		counts[w]++ // zero value của int là 0, nên ok khi key chưa tồn tại
		fmt.Printf("count of %s: %d\n", w, counts[w])
	}
	fmt.Println("counts:", counts)

	// Map of slices
	groups := map[string][]string{
		"backend":  {"go", "rust"},
		"frontend": {"js", "ts"},
	}
	groups["backend"] = append(groups["backend"], "python")
	fmt.Println("groups:", groups)

	// TODO: Đếm tần suất từng từ trong câu sau:
	sentence := "the quick brown fox jumps over the lazy dog the fox"
	// Dùng strings.Fields(sentence) để tách từ
	words = strings.Split(sentence, " ")
	wordCounts := make(map[string]int)
	// In ra map và tìm từ xuất hiện nhiều nhất
	for _, w := range words {
		w = strings.Trim(w, " ") // loại bỏ dấu câu
		wordCounts[w]++
	}
	fmt.Println("word counts:", wordCounts)
}
