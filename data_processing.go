// data_processing.go
// Comprehensive Data Processing Techniques in Go (500 lines)

package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"
)

// 1. Basic Data Structures
func basicDataStructures() {
	// Arrays
	var arr [5]int
	arr[0] = 1
	fmt.Println("Array:", arr)

	// Slices
	slice := []int{1, 2, 3}
	slice = append(slice, 4)
	fmt.Println("Slice:", slice)

	// Maps
	m := map[string]int{"a": 1, "b": 2}
	m["c"] = 3
	fmt.Println("Map:", m)

	// Structs
	type Person struct {
		Name string
		Age  int
	}
	p := Person{"Alice", 30}
	fmt.Println("Struct:", p)
}

// 2. Sorting Data
func sortingData() {
	// Sorting slices
	nums := []int{5, 2, 6, 3, 1, 4}
	sort.Ints(nums)
	fmt.Println("Sorted numbers:", nums)

	// Sorting strings
	names := []string{"Charlie", "Alice", "Bob"}
	sort.Strings(names)
	fmt.Println("Sorted names:", names)

	// Custom sorting
	type Person struct {
		Name string
		Age  int
	}
	people := []Person{
		{"Bob", 31},
		{"Alice", 25},
		{"Charlie", 30},
	}
	sort.Slice(people, func(i, j int) bool {
		return people[i].Age < people[j].Age
	})
	fmt.Println("Sorted by age:", people)
}

// 3. Filtering Data
func filteringData() {
	nums := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	// Filter even numbers
	evens := filter(nums, func(n int) bool {
		return n%2 == 0
	})
	fmt.Println("Even numbers:", evens)

	// Filter numbers greater than 5
	largeNums := filter(nums, func(n int) bool {
		return n > 5
	})
	fmt.Println("Numbers > 5:", largeNums)
}

func filter[T any](slice []T, test func(T) bool) []T {
	var result []T
	for _, item := range slice {
		if test(item) {
			result = append(result, item)
		}
	}
	return result
}

// 4. Mapping Data
func mappingData() {
	nums := []int{1, 2, 3, 4, 5}

	// Square numbers
	squares := mapSlice(nums, func(n int) int {
		return n * n
	})
	fmt.Println("Squares:", squares)

	// Convert to strings
	strs := mapSlice(nums, func(n int) string {
		return fmt.Sprintf("Number %d", n)
	})
	fmt.Println("Strings:", strs)
}

func mapSlice[T any, U any](slice []T, f func(T) U) []U {
	result := make([]U, len(slice))
	for i, item := range slice {
		result[i] = f(item)
	}
	return result
}

// 5. Reducing Data
func reducingData() {
	nums := []int{1, 2, 3, 4, 5}

	// Sum numbers
	sum := reduce(nums, 0, func(acc, curr int) int {
		return acc + curr
	})
	fmt.Println("Sum:", sum)

	// Product of numbers
	product := reduce(nums, 1, func(acc, curr int) int {
		return acc * curr
	})
	fmt.Println("Product:", product)
}

func reduce[T any, U any](slice []T, initial U, f func(U, T) U) U {
	result := initial
	for _, item := range slice {
		result = f(result, item)
	}
	return result
}

// 6. CSV Processing
func csvProcessing() {
	data := `name,age,city
Alice,30,New York
Bob,25,Los Angeles
Charlie,35,Chicago`

	r := csv.NewReader(strings.NewReader(data))
	records, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	// Convert to map
	var result []map[string]string
	headers := records[0]
	for _, record := range records[1:] {
		entry := make(map[string]string)
		for i, value := range record {
			entry[headers[i]] = value
		}
		result = append(result, entry)
	}

	fmt.Println("CSV data:")
	for _, entry := range result {
		fmt.Printf("%+v\n", entry)
	}

	// Write CSV
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	w.WriteAll(records)
	w.Flush()
	fmt.Println("CSV output:")
	fmt.Println(buf.String())
}

// 7. JSON Processing
func jsonProcessing() {
	type Person struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
		City string `json:"city,omitempty"`
	}

	// Marshal
	p := Person{Name: "Alice", Age: 30}
	jsonData, err := json.Marshal(p)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("JSON:", string(jsonData))

	// Unmarshal
	var p2 Person
	err = json.Unmarshal(jsonData, &p2)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Struct: %+v\n", p2)

	// Streaming decoder
	const data = `{"name":"Bob","age":25}
{"name":"Charlie","age":35}`
	dec := json.NewDecoder(strings.NewReader(data))
	for {
		var person Person
		if err := dec.Decode(&person); err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Streamed: %+v\n", person)
	}
}

// 8. Data Validation
func dataValidation() {
	type User struct {
		Username string
		Email    string
		Age      int
	}

	validate := func(u User) error {
		var errs []string
		if len(u.Username) < 3 {
			errs = append(errs, "username too short")
		}
		if !strings.Contains(u.Email, "@") {
			errs = append(errs, "invalid email")
		}
		if u.Age < 18 {
			errs = append(errs, "age must be 18+")
		}
		if len(errs) > 0 {
			return fmt.Errorf("validation errors: %s", strings.Join(errs, ", "))
		}
		return nil
	}

	users := []User{
		{"al", "alice@example.com", 20},
		{"bob", "invalid-email", 15},
		{"charlie", "charlie@example.com", 25},
	}

	for _, u := range users {
		if err := validate(u); err != nil {
			fmt.Printf("Invalid user %s: %v\n", u.Username, err)
		} else {
			fmt.Printf("Valid user: %s\n", u.Username)
		}
	}
}

// 9. Data Aggregation
func dataAggregation() {
	type Sale struct {
		Date   time.Time
		Amount float64
		Region string
	}

	sales := []Sale{
		{time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC), 100.50, "East"},
		{time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC), 200.75, "West"},
		{time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC), 50.25, "East"},
		{time.Date(2023, 1, 3, 0, 0, 0, 0, time.UTC), 300.00, "North"},
	}

	// Group by date
	byDate := make(map[time.Time]float64)
	for _, sale := range sales {
		byDate[sale.Date] += sale.Amount
	}
	fmt.Println("Sales by date:", byDate)

	// Group by region
	byRegion := make(map[string]float64)
	for _, sale := range sales {
		byRegion[sale.Region] += sale.Amount
	}
	fmt.Println("Sales by region:", byRegion)

	// Average by region
	regionCounts := make(map[string]int)
	regionTotals := make(map[string]float64)
	for _, sale := range sales {
		regionCounts[sale.Region]++
		regionTotals[sale.Region] += sale.Amount
	}
	averages := make(map[string]float64)
	for region := range regionCounts {
		averages[region] = regionTotals[region] / float64(regionCounts[region])
	}
	fmt.Println("Average by region:", averages)
}

// 10. Data Transformation Pipeline
func dataTransformationPipeline() {
	type Employee struct {
		Name     string
		Salary   float64
		Position string
	}

	employees := []Employee{
		{"Alice", 75000, "Developer"},
		{"Bob", 85000, "Manager"},
		{"Charlie", 60000, "Developer"},
		{"David", 90000, "Director"},
	}

	// Pipeline steps
	developersOnly := filter(employees, func(e Employee) bool {
		return e.Position == "Developer"
	})

	addBonus := mapSlice(developersOnly, func(e Employee) Employee {
		return Employee{
			Name:     e.Name,
			Salary:   e.Salary * 1.10, // 10% bonus
			Position: e.Position,
		}
	})

	sortBySalary := make([]Employee, len(addBonus))
	copy(sortBySalary, addBonus)
	sort.Slice(sortBySalary, func(i, j int) bool {
		return sortBySalary[i].Salary > sortBySalary[j].Salary
	})

	fmt.Println("Processed employees:")
	for _, e := range sortBySalary {
		fmt.Printf("%s: %.2f\n", e.Name, e.Salary)
	}
}

// 11. Concurrent Data Processing
func concurrentDataProcessing() {
	type Product struct {
		ID    int
		Price float64
	}

	products := make([]Product, 100)
	for i := range products {
		products[i] = Product{ID: i + 1, Price: float64((i + 1) * 10)}
	}

	// Process products concurrently
	var wg sync.WaitGroup
	resultChan := make(chan float64, 10)

	process := func(p Product) {
		defer wg.Done()
		// Simulate processing
		time.Sleep(10 * time.Millisecond)
		// Apply discount
		discounted := p.Price * 0.9
		resultChan <- discounted
	}

	// Start workers
	for _, p := range products {
		wg.Add(1)
		go process(p)
	}

	// Wait for completion
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	var total float64
	for discounted := range resultChan {
		total += discounted
	}

	fmt.Printf("Total discounted value: %.2f\n", total)
}

// 12. Data Sampling
func dataSampling() {
	data := make([]float64, 1000)
	for i := range data {
		data[i] = float64(i) + rand.Float64()
	}

	// Simple random sample
	sampleSize := 100
	sample := make([]float64, sampleSize)
	for i := 0; i < sampleSize; i++ {
		idx := rand.Intn(len(data))
		sample[i] = data[idx]
	}

	// Calculate statistics
	mean := reduce(sample, 0.0, func(acc, curr float64) float64 {
		return acc + curr
	}) / float64(sampleSize)

	variance := reduce(sample, 0.0, func(acc, curr float64) float64 {
		return acc + (curr-mean)*(curr-mean)
	}) / float64(sampleSize)

	stddev := math.Sqrt(variance)

	fmt.Printf("Sample stats: mean=%.2f, stddev=%.2f\n", mean, stddev)
}

// 13. Data Caching
type DataCache struct {
	mu    sync.RWMutex
	cache map[string]interface{}
}

func NewDataCache() *DataCache {
	return &DataCache{
		cache: make(map[string]interface{}),
	}
}

func (dc *DataCache) Get(key string) (interface{}, bool) {
	dc.mu.RLock()
	defer dc.mu.RUnlock()
	val, ok := dc.cache[key]
	return val, ok
}

func (dc *DataCache) Set(key string, value interface{}) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	dc.cache[key] = value
}

func dataCaching() {
	cache := NewDataCache()

	// Expensive computation
	compute := func(key string) interface{} {
		time.Sleep(500 * time.Millisecond)
		return strings.ToUpper(key)
	}

	getOrCompute := func(key string) interface{} {
		if val, ok := cache.Get(key); ok {
			return val
		}
		val := compute(key)
		cache.Set(key, val)
		return val
	}

	start := time.Now()
	fmt.Println(getOrCompute("hello"))
	fmt.Println("First call took:", time.Since(start))

	start = time.Now()
	fmt.Println(getOrCompute("hello"))
	fmt.Println("Second call took:", time.Since(start))
}

// 14. Data Batching
func dataBatching() {
	data := make([]int, 100)
	for i := range data {
		data[i] = i + 1
	}

	batchSize := 10
	batches := make([][]int, 0, len(data)/batchSize+1)

	for batchSize < len(data) {
		data, batches = data[batchSize:], append(batches, data[0:batchSize:batchSize])
	}
	batches = append(batches, data)

	// Process batches
	for i, batch := range batches {
		sum := reduce(batch, 0, func(acc, curr int) int {
			return acc + curr
		})
		fmt.Printf("Batch %d: sum=%d\n", i+1, sum)
	}
}

// 15. Data Compression
func dataCompression() {
	original := strings.Repeat("Go is awesome! ", 100)
	fmt.Println("Original size:", len(original))

	// Simple "compression" by removing spaces (just for demonstration)
	compressed := strings.ReplaceAll(original, " ", "")
	fmt.Println("Compressed size:", len(compressed))

	// "Decompression"
	decompressed := strings.ReplaceAll(compressed, "!", " !")
	fmt.Println("Decompressed matches original:",
		len(decompressed) == len(original))
}

func main() {
	basicDataStructures()
	sortingData()
	filteringData()
	mappingData()
	reducingData()
	csvProcessing()
	jsonProcessing()
	dataValidation()
	dataAggregation()
	dataTransformationPipeline()
	concurrentDataProcessing()
	dataSampling()
	dataCaching()
	dataBatching()
	dataCompression()
}
