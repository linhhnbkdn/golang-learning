package main

import "fmt"

// ============================================================
// Ví dụ: Hệ thống thông báo — Email, SMS, Push Notification
// ============================================================

// INTERFACE — chỉ định "cần làm được gì"
// Không quan tâm Email hay SMS hay Push, miễn Send được là dùng
type Notifier interface {
	Send(message string) error
	Name() string
}

// ============================================================
// STRUCT 1 — Email
// ============================================================
type EmailNotifier struct {
	ToAddress string
	FromName  string
}

func (e EmailNotifier) Send(message string) error {
	fmt.Printf("[Email] To: %s | From: %s | Msg: %s\n", e.ToAddress, e.FromName, message)
	return nil
}

func (e EmailNotifier) Name() string { return "Email" }

// Method riêng của Email, không có trong interface
func (e EmailNotifier) AttachFile(filename string) {
	fmt.Printf("[Email] Attached: %s\n", filename)
}

// ============================================================
// STRUCT 2 — SMS
// ============================================================
type SMSNotifier struct {
	PhoneNumber string
}

func (s SMSNotifier) Send(message string) error {
	if len(message) > 160 {
		return fmt.Errorf("SMS too long: %d chars (max 160)", len(message))
	}
	fmt.Printf("[SMS] To: %s | Msg: %s\n", s.PhoneNumber, message)
	return nil
}

func (s SMSNotifier) Name() string { return "SMS" }

// ============================================================
// STRUCT 3 — Push Notification
// ============================================================
type PushNotifier struct {
	DeviceToken string
	AppName     string
}

func (p PushNotifier) Send(message string) error {
	fmt.Printf("[Push] App: %s | Token: %s... | Msg: %s\n", p.AppName, p.DeviceToken[:8], message)
	return nil
}

func (p PushNotifier) Name() string { return "Push" }

// ============================================================
// Hàm nhận INTERFACE — không cần biết loại cụ thể
// ============================================================
func notify(n Notifier, message string) {
	fmt.Printf("Sending via %s...\n", n.Name())
	if err := n.Send(message); err != nil {
		fmt.Printf("  ERROR: %v\n", err)
	}
}

// Gửi đến nhiều kênh cùng lúc
func broadcast(notifiers []Notifier, message string) {
	fmt.Println("=== Broadcasting ===")
	for _, n := range notifiers {
		notify(n, message)
	}
}

func main() {
	// Tạo các struct cụ thể
	email := EmailNotifier{ToAddress: "li@example.com", FromName: "System"}
	sms := SMSNotifier{PhoneNumber: "+84901234567"}
	push := PushNotifier{DeviceToken: "abc123xyz456789", AppName: "MyApp"}

	// Dùng method riêng của struct — chỉ Email có AttachFile
	email.AttachFile("report.pdf")
	fmt.Println()

	// Dùng qua interface — cùng 1 hàm, 3 loại khác nhau
	notify(email, "Hello Li!")
	notify(sms, "Hello Li!")
	notify(push, "Hello Li!")
	fmt.Println()

	// Lưu nhiều loại vào 1 slice vì cùng implement Notifier
	channels := []Notifier{email, sms, push}
	broadcast(channels, "Server is down!")
	fmt.Println()

	// SMS có giới hạn 160 ký tự — error handling
	longMsg := "This is a very long message that exceeds the SMS character limit of 160 characters. It will definitely fail and return an error from the SMS notifier."
	notify(sms, longMsg)
}
