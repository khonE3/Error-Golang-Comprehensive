// testing_benchmarking.go
// Comprehensive Testing and Benchmarking in Go (500 lines)

package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

// 1. Unit Tests
func TestAdd(t *testing.T) {
	tests := []struct {
		name     string
		a, b     int
		expected int
	}{
		{"positive", 2, 3, 5},
		{"zero", 0, 0, 0},
		{"negative", -1, -1, -2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := add(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("add(%d, %d) = %d; want %d", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func add(a, b int) int {
	return a + b
}

// 2. Table-Driven Tests
func TestDivide(t *testing.T) {
	tests := []struct {
		name        string
		a, b        float64
		expected    float64
		expectError bool
	}{
		{"normal division", 10.0, 2.0, 5.0, false},
		{"divide by zero", 10.0, 0.0, 0.0, true},
		{"negative numbers", -10.0, 2.0, -5.0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := divide(tt.a, tt.b)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if result != tt.expected {
					t.Errorf("divide(%f, %f) = %f; want %f", tt.a, tt.b, result, tt.expected)
				}
			}
		})
	}
}

func divide(a, b float64) (float64, error) {
	if b == 0.0 {
		return 0.0, errors.New("division by zero")
	}
	return a / b, nil
}

// 3. Mocking Interfaces
type DataStore interface {
	GetValue(key string) (int, error)
	SetValue(key string, value int) error
}

type Processor struct {
	store DataStore
}

func (p *Processor) Process(key string) error {
	value, err := p.store.GetValue(key)
	if err != nil {
		return err
	}

	newValue := value * 2
	return p.store.SetValue(key, newValue)
}

type MockStore struct {
	data  map[string]int
	calls []string
}

func (m *MockStore) GetValue(key string) (int, error) {
	m.calls = append(m.calls, "GetValue:"+key)
	val, exists := m.data[key]
	if !exists {
		return 0, errors.New("key not found")
	}
	return val, nil
}

func (m *MockStore) SetValue(key string, value int) error {
	m.calls = append(m.calls, fmt.Sprintf("SetValue:%s:%d", key, value))
	m.data[key] = value
	return nil
}

func TestProcessor(t *testing.T) {
	mockStore := &MockStore{
		data:  map[string]int{"test": 10},
		calls: []string{},
	}
	processor := &Processor{store: mockStore}

	err := processor.Process("test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedCalls := []string{
		"GetValue:test",
		"SetValue:test:20",
	}
	if len(mockStore.calls) != len(expectedCalls) {
		t.Errorf("expected %d calls, got %d", len(expectedCalls), len(mockStore.calls))
	}
	for i, call := range expectedCalls {
		if mockStore.calls[i] != call {
			t.Errorf("call %d: expected %q, got %q", i, call, mockStore.calls[i])
		}
	}

	if mockStore.data["test"] != 20 {
		t.Errorf("expected value 20, got %d", mockStore.data["test"])
	}
}

// 4. Test Helpers
func createTempFile(t *testing.T, content string) string {
	t.Helper()

	tmpfile, err := os.CreateTemp("", "example")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tmpfile.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	return tmpfile.Name()
}

func TestFileProcessing(t *testing.T) {
	testContent := "line1\nline2\nline3"
	filename := createTempFile(t, testContent)
	defer os.Remove(filename)

	lines, err := countLines(filename)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if lines != 3 {
		t.Errorf("expected 3 lines, got %d", lines)
	}
}

func countLines(filename string) (int, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return 0, err
	}
	return len(strings.Split(string(content), "\n")), nil
}

// 5. Benchmark Tests
func BenchmarkFibonacci(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fibonacci(20)
	}
}

func fibonacci(n int) int {
	if n <= 1 {
		return n
	}
	return fibonacci(n-1) + fibonacci(n-2)
}

func BenchmarkFibonacciMemoized(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fibonacciMemoized(20)
	}
}

var fibCache = make(map[int]int)

func fibonacciMemoized(n int) int {
	if val, exists := fibCache[n]; exists {
		return val
	}

	if n <= 1 {
		fibCache[n] = n
		return n
	}

	val := fibonacciMemoized(n-1) + fibonacciMemoized(n-2)
	fibCache[n] = val
	return val
}

// 6. Parallel Testing
func TestParallel(t *testing.T) {
	tests := []struct {
		name string
		val  int
	}{
		{"test1", 1},
		{"test2", 2},
		{"test3", 3},
		{"test4", 4},
	}

	for _, tt := range tests {
		tt := tt // capture range variable
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			time.Sleep(time.Duration(tt.val) * 100 * time.Millisecond)
			if tt.val%2 == 0 {
				t.Logf("Even value: %d", tt.val)
			} else {
				t.Logf("Odd value: %d", tt.val)
			}
		})
	}
}

// 7. Test Main
func TestMain(m *testing.M) {
	fmt.Println("Setting up test environment")
	start := time.Now()

	code := m.Run()

	fmt.Printf("Tests completed in %v\n", time.Since(start))
	os.Exit(code)
}

// 8. Example Tests
func ExampleAdd() {
	sum := add(2, 3)
	fmt.Println(sum)
	// Output: 5
}

// 9. Fuzz Testing
func FuzzReverse(f *testing.F) {
	testcases := []string{"Hello, world", " ", "!12345"}
	for _, tc := range testcases {
		f.Add(tc)
	}

	f.Fuzz(func(t *testing.T, orig string) {
		rev := reverseString(orig)
		doubleRev := reverseString(rev)
		if orig != doubleRev {
			t.Errorf("Before: %q, after: %q", orig, doubleRev)
		}

		if utf8.ValidString(orig) && !utf8.ValidString(rev) {
			t.Errorf("Reverse produced invalid UTF-8 string %q", rev)
		}
	})
}

func reverseString(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < len(r)/2; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}

// 10. Test Coverage
func TestCoverage(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"hello", "HELLO"},
		{"world", "WORLD"},
		{"", ""},
	}

	for _, tt := range tests {
		result := toUpper(tt.input)
		if result != tt.expected {
			t.Errorf("toUpper(%q) = %q; want %q", tt.input, result, tt.expected)
		}
	}
}

func toUpper(s string) string {
	return strings.ToUpper(s)
}

// 11. Golden File Tests
func TestGoldenFile(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		golden string
	}{
		{"basic", "hello", "testdata/basic.golden"},
		{"empty", "", "testdata/empty.golden"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := processString(tt.input)

			// Update golden files (run with -update flag)
			if *update {
				os.WriteFile(tt.golden, []byte(result), 0644)
				return
			}

			expected, err := os.ReadFile(tt.golden)
			if err != nil {
				t.Fatal(err)
			}

			if result != string(expected) {
				t.Errorf("got %q, want %q", result, string(expected))
			}
		})
	}
}

var update = flag.Bool("update", false, "update golden files")

func processString(s string) string {
	return strings.ToUpper(s) + " processed"
}

// 12. Benchmark Comparison
func BenchmarkStringConcatenation(b *testing.B) {
	b.Run("plus", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s := ""
			for j := 0; j < 100; j++ {
				s += "a"
			}
		}
	})

	b.Run("builder", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var builder strings.Builder
			for j := 0; j < 100; j++ {
				builder.WriteString("a")
			}
			_ = builder.String()
		}
	})

	b.Run("bytes", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			for j := 0; j < 100; j++ {
				buf.WriteString("a")
			}
			_ = buf.String()
		}
	})
}

// 13. Race Detection
func TestRaceCondition(t *testing.T) {
	var counter int
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			counter++
		}()
	}

	wg.Wait()
	if counter != 100 {
		t.Errorf("counter = %d; want 100", counter)
	}
}

// 14. Test Cleanup
func TestWithCleanup(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "test.txt")

	t.Logf("Using temp dir: %s", tempDir)

	// Write to file
	err := os.WriteFile(filePath, []byte("test content"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Verify file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Errorf("file %s does not exist", filePath)
	}

	// Cleanup is automatic with t.TempDir()
}

// 15. HTTP Test Server
func TestHTTPHandler(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		if name == "" {
			name = "World"
		}
		fmt.Fprintf(w, "Hello, %s!", name)
	})

	ts := httptest.NewServer(handler)
	defer ts.Close()

	res, err := http.Get(ts.URL + "?name=Test")
	if err != nil {
		t.Fatal(err)
	}

	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		t.Fatal(err)
	}

	expected := "Hello, Test!"
	if string(body) != expected {
		t.Errorf("got %q; want %q", body, expected)
	}
}

func main() {
	// This file is primarily for testing
	// Run tests with: go test -v
	fmt.Println("Run 'go test' to execute the tests")
}
