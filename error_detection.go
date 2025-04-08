
// Comprehensive Error Detection and Handling in Go 

package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// 1. Basic Error Handling
func basicErrorHandling() {
	// Simple error creation
	err := errors.New("something went wrong")
	if err != nil {
		fmt.Println("Basic error:", err)
	}

	// Common pattern: error as return value
	result, err := divide(10, 0)
	if err != nil {
		fmt.Println("Division error:", err)
	} else {
		fmt.Println("Result:", result)
	}
}

func divide(a, b float64) (float64, error) {
	if b == 0 {
		return 0, errors.New("division by zero")
	}
	return a / b, nil
}

// 2. Custom Error Types
type CustomError struct {
	Code    int
	Message string
	When    time.Time
}

func (e *CustomError) Error() string {
	return fmt.Sprintf("[%d] %s (at %v)", e.Code, e.Message, e.When)
}

func testCustomError() error {
	return &CustomError{
		Code:    500,
		Message: "internal server error",
		When:    time.Now(),
	}
}

// 3. Error Wrapping and Inspection
func errorWrapping() {
	originalErr := errors.New("original error")
	wrappedErr := fmt.Errorf("context: %w", originalErr)

	fmt.Println("Wrapped error:", wrappedErr)

	// Unwrapping
	if unwrapped := errors.Unwarp(wrappedErr); unwrapped != nil {
		fmt.Println("Unwrapped:", unwrapped)
	}

	// Checking error types
	var customErr *CustomError
	if errors.As(wrappedErr, &customErr) {
		fmt.Println("Is CustomError:", customErr)
	}
}

// 4. Error Handling Patterns
func errorHandlingPatterns() {
	// Pattern 1: Sentinel errors
	if _, err := strconv.Atoi("abc"); err != nil {
		if errors.Is(err, strconv.ErrSyntax) {
			fmt.Println("Invalid syntax for number conversion")
		}
	}

	// Pattern 2: Error chains
	if err := processFile("nonexistent.txt"); err != nil {
		fmt.Printf("Processing failed: %v\n", err)
		fmt.Printf("Full details: %+v\n", err)
	}

	// Pattern 3: Retry with exponential backoff
	if err := retryOperation(3); err != nil {
		fmt.Println("Operation failed after retries:", err)
	}
}

func processFile(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	if len(data) == 0 {
		return fmt.Errorf("empty file: %w", errors.New("invalid content"))
	}

	return nil
}

func retryOperation(maxAttempts int) error {
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		if err := someUnreliableOperation(); err == nil {
			return nil
		} else {
			lastErr = err
			time.Sleep(time.Second * time.Duration(i+1))
		}
	}
	return fmt.Errorf("after %d attempts: %w", maxAttempts, lastErr)
}

func someUnreliableOperation() error {
	// Simulate unreliable operation
	if time.Now().UnixNano()%2 == 0 {
		return nil
	}
	return errors.New("temporary failure")
}

// 5. Panic and Recovery
func panicAndRecovery() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from panic:", r)
		}
	}()

	// This will panic
	panic("something terrible happened")
}

// 6. Logging Errors
func errorLogging() {
	file, err := os.Open("config.json")
	if err != nil {
		log.Printf("Failed to open config: %v", err)
		log.Fatalf("Fatal error: %v", err)
	}
	defer file.Close()
}

// 7. Error Handling in Concurrent Code
func concurrentErrorHandling() {
	errChan := make(chan error, 2)

	go func() {
		_, err := strconv.Atoi("abc")
		errChan <- err
	}()

	go func() {
		_, err := os.Open("nonexistent.txt")
		errChan <- err
	}()

	for i := 0; i < 2; i++ {
		if err := <-errChan; err != nil {
			fmt.Printf("Goroutine error: %v\n", err)
		}
	}
}

// 8. Error Handling Middleware (HTTP example)
type HandlerFunc func(w http.ResponseWriter, r *http.Request) error

func errorMiddleware(h HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := h(w, r); err != nil {
			// Handle different error types appropriately
			var status int
			switch {
			case errors.Is(err, ErrNotFound):
				status = http.StatusNotFound
			case errors.Is(err, ErrUnauthorized):
				status = http.StatusUnauthorized
			default:
				status = http.StatusInternalServerError
			}
			http.Error(w, err.Error(), status)
		}
	}
}

var (
	ErrNotFound     = errors.New("not found")
	ErrUnauthorized = errors.New("unauthorized")
)

// 9. Benchmarking Error Creation
func benchmarkErrorCreation() {
	start := time.Now()
	for i := 0; i < 100000; i++ {
		_ = errors.New("test error")
	}
	fmt.Printf("Basic errors: %v\n", time.Since(start))

	start = time.Now()
	for i := 0; i < 100000; i++ {
		_ = fmt.Errorf("test error %d", i)
	}
	fmt.Printf("Formatted errors: %v\n", time.Since(start))
}

// 10. Advanced Error Handling Techniques
func advancedTechniques() {
	// Technique 1: Error aggregation
	var errs []error
	for i := 0; i < 3; i++ {
		if err := someOperation(i); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		fmt.Println("Aggregated errors:", joinErrors(errs))
	}

	// Technique 2: Temporary errors
	if err := networkOperation(); err != nil {
		if isTemporary(err) {
			fmt.Println("Temporary error, will retry:", err)
		} else {
			fmt.Println("Permanent error:", err)
		}
	}
}

func someOperation(i int) error {
	if i%2 == 0 {
		return fmt.Errorf("operation failed for %d", i)
	}
	return nil
}

func joinErrors(errs []error) error {
	var msgs []string
	for _, err := range errs {
		msgs = append(msgs, err.Error())
	}
	return errors.New(strings.Join(msgs, "; "))
}

type temporary interface {
	Temporary() bool
}

func isTemporary(err error) bool {
	te, ok := err.(temporary)
	return ok && te.Temporary()
}

func networkOperation() error {
	return &networkError{temp: true}
}

type networkError struct {
	temp bool
}

func (e *networkError) Error() string {
	return "network error"
}

func (e *networkError) Temporary() bool {
	return e.temp
}

// 11. Error Handling Best Practices
func bestPractices() {
	// Practice 1: Always handle errors
	_, err := os.Open("file.txt")
	if err != nil {
		// Handle error properly, don't ignore
	}

	// Practice 2: Add context to errors
	if err := processData(); err != nil {
		// Wrap with additional context
		fmt.Printf("main: %v\n", fmt.Errorf("processing failed: %w", err))
	}

	// Practice 3: Use sentinel errors for expected cases
	if _, err := findUser("missing"); err != nil {
		if errors.Is(err, ErrUserNotFound) {
			fmt.Println("User not found (expected case)")
		} else {
			fmt.Println("Unexpected error:", err)
		}
	}

	// Practice 4: Document possible errors in function signatures
}

var ErrUserNotFound = errors.New("user not found")

func findUser(id string) (string, error) {
	return "", ErrUserNotFound
}

func processData() error {
	return errors.New("data processing error")
}

// 12. Testing Error Conditions
func testErrorConditions() {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid", "123", false},
		{"invalid", "abc", true},
	}

	for _, tt := range tests {
		_, err := strconv.Atoi(tt.input)
		if (err != nil) != tt.wantErr {
			fmt.Printf("%s: unexpected error status: %v\n", tt.name, err)
		}
	}
}

// 13. Error Handling in Defer
func errorInDefer() error {
	file, err := os.Create("test.txt")
	if err != nil {
		return err
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Println("Error closing file:", err)
		}
	}()

	_, err = file.WriteString("data")
	return err
}

// 14. Error Handling with Context
func errorWithContext() {
	if err := performTask(); err != nil {
		fmt.Printf("Task failed: %v\n", err)
		fmt.Printf("Error type: %T\n", err)
	}
}

func performTask() error {
	if err := step1(); err != nil {
		return fmt.Errorf("step1: %w", err)
	}
	if err := step2(); err != nil {
		return fmt.Errorf("step2: %w", err)
	}
	return nil
}

func step1() error {
	return errors.New("step1 failed")
}

func step2() error {
	return nil
}

// 15. Comprehensive Example: File Processor
type FileProcessor struct {
	skipErrors bool
	logger     *log.Logger
}

func (fp *FileProcessor) ProcessFiles(filenames []string) error {
	var errs []error

	for _, filename := range filenames {
		if err := fp.processFile(filename); err != nil {
			if fp.skipErrors {
				fp.logger.Printf("Skipping %s: %v", filename, err)
				continue
			}
			errs = append(errs, fmt.Errorf("%s: %w", filename, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("%d errors occurred: %v", len(errs), joinErrors(errs))
	}
	return nil
}

func (fp *FileProcessor) processFile(filename string) error {
	content, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("read failed: %w", err)
	}

	if len(content) > 1000 {
		return errors.New("file too large")
	}

	// Simulate processing
	if strings.Contains(string(content), "error") {
		return errors.New("invalid content detected")
	}

	return nil
}

func main() {
	// Execute all error handling examples
	basicErrorHandling()
	
	if err := testCustomError(); err != nil {
		fmt.Println("Custom error:", err)
	}
	
	errorWrapping()
	errorHandlingPatterns()
	panicAndRecovery()
	errorLogging()
	concurrentErrorHandling()
	benchmarkErrorCreation()
	advancedTechniques()
	bestPractices()
	testErrorConditions()
	errorInDefer()
	errorWithContext()
	
	// File processor example
	fp := &FileProcessor{
		skipErrors: true,
		logger:     log.New(os.Stdout, "FILE: ", log.LstdFlags),
	}
	files := []string{"file1.txt", "file2.txt", "file3.txt"}
	if err := fp.ProcessFiles(files); err != nil {
		fmt.Println("File processing failed:", err)
	}
}