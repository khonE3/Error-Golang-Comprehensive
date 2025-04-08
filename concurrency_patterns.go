// concurrency_patterns.go
// Comprehensive Guide to Concurrency Patterns in Go (500 lines)

package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// 1. Basic Goroutines and WaitGroup
func basicConcurrency() {
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
			fmt.Printf("Goroutine %d completed\n", id)
		}(i)
	}

	wg.Wait()
	fmt.Println("All goroutines finished")
}

// 2. Channel Patterns
func channelPatterns() {
	// Buffered vs unbuffered channels
	unbuffered := make(chan int)
	buffered := make(chan int, 10)

	// Channel directions
	producer := func(ch chan<- int) {
		for i := 0; i < 5; i++ {
			ch <- i
		}
		close(ch)
	}

	consumer := func(ch <-chan int) {
		for v := range ch {
			fmt.Println("Received:", v)
		}
	}

	go producer(buffered)
	consumer(buffered)

	// Select statement
	ch1 := make(chan string)
	ch2 := make(chan string)

	go func() {
		time.Sleep(100 * time.Millisecond)
		ch1 <- "one"
	}()

	go func() {
		time.Sleep(200 * time.Millisecond)
		ch2 <- "two"
	}()

	for i := 0; i < 2; i++ {
		select {
		case msg := <-ch1:
			fmt.Println("Received", msg)
		case msg := <-ch2:
			fmt.Println("Received", msg)
		case <-time.After(300 * time.Millisecond):
			fmt.Println("Timeout")
		}
	}
}

// 3. Worker Pool Pattern
func workerPool() {
	const numJobs = 10
	const numWorkers = 3

	jobs := make(chan int, numJobs)
	results := make(chan int, numJobs)

	// Worker function
	worker := func(id int, jobs <-chan int, results chan<- int) {
		for j := range jobs {
			fmt.Printf("Worker %d processing job %d\n", id, j)
			time.Sleep(time.Second)
			results <- j * 2
		}
	}

	// Start workers
	for w := 1; w <= numWorkers; w++ {
		go worker(w, jobs, results)
	}

	// Send jobs
	for j := 1; j <= numJobs; j++ {
		jobs <- j
	}
	close(jobs)

	// Collect results
	for a := 1; a <= numJobs; a++ {
		fmt.Println("Result:", <-results)
	}
}

// 4. Fan-out, Fan-in Pattern
func fanOutFanIn() {
	producer := func() <-chan int {
		out := make(chan int)
		go func() {
			defer close(out)
			for i := 0; i < 10; i++ {
				out <- i
			}
		}()
		return out
	}

	processor := func(in <-chan int) <-chan int {
		out := make(chan int)
		go func() {
			defer close(out)
			for n := range in {
				out <- n * n
			}
		}()
		return out
	}

	merge := func(cs ...<-chan int) <-chan int {
		var wg sync.WaitGroup
		out := make(chan int)

		output := func(c <-chan int) {
			defer wg.Done()
			for n := range c {
				out <- n
			}
		}

		wg.Add(len(cs))
		for _, c := range cs {
			go output(c)
		}

		go func() {
			wg.Wait()
			close(out)
		}()
		return out
	}

	in := producer()

	// Fan-out: Distribute work across multiple processors
	c1 := processor(in)
	c2 := processor(in)
	c3 := processor(in)

	// Fan-in: Merge results from multiple channels
	for result := range merge(c1, c2, c3) {
		fmt.Println("Processed:", result)
	}
}

// 5. Pipeline Pattern
func pipelinePattern() {
	gen := func(nums ...int) <-chan int {
		out := make(chan int)
		go func() {
			for _, n := range nums {
				out <- n
			}
			close(out)
		}()
		return out
	}

	sq := func(in <-chan int) <-chan int {
		out := make(chan int)
		go func() {
			for n := range in {
				out <- n * n
			}
			close(out)
		}()
		return out
	}

	sum := func(in <-chan int) <-chan int {
		out := make(chan int)
		go func() {
			total := 0
			for n := range in {
				total += n
			}
			out <- total
			close(out)
		}()
		return out
	}

	nums := []int{1, 2, 3, 4, 5}
	total := <-sum(sq(gen(nums...)))
	fmt.Println("Pipeline result:", total)
}

// 6. Context Pattern for Cancellation
func contextPattern() {
	worker := func(ctx context.Context, id int) error {
		select {
		case <-time.After(time.Second * time.Duration(id)):
			fmt.Printf("Worker %d completed\n", id)
			return nil
		case <-ctx.Done():
			fmt.Printf("Worker %d canceled: %v\n", id, ctx.Err())
			return ctx.Err()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := worker(ctx, id); err != nil {
				fmt.Printf("Error in worker %d: %v\n", id, err)
			}
		}(i)
	}
	wg.Wait()
}

// 7. Mutex Patterns
func mutexPatterns() {
	var counter int
	var mu sync.Mutex

	increment := func() {
		mu.Lock()
		defer mu.Unlock()
		counter++
	}

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			increment()
		}()
	}
	wg.Wait()

	fmt.Println("Final counter:", counter)
}

// 8. RWMutex Pattern
func rwMutexPattern() {
	var cache = make(map[string]string)
	var rwMu sync.RWMutex

	get := func(key string) string {
		rwMu.RLock()
		defer rwMu.RUnlock()
		return cache[key]
	}

	set := func(key, value string) {
		rwMu.Lock()
		defer rwMu.Unlock()
		cache[key] = value
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", i)
			set(key, fmt.Sprintf("value%d", i))
		}(i)
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", i)
			fmt.Println("Got:", get(key))
		}(i)
	}

	wg.Wait()
}

// 9. Once Pattern
func oncePattern() {
	var once sync.Once
	var initialized bool

	initialize := func() {
		initialized = true
		fmt.Println("Initialized")
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			once.Do(initialize)
		}()
	}
	wg.Wait()

	fmt.Println("Initialized status:", initialized)
}

// 10. Pool Pattern
func poolPattern() {
	var pool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 1024)
		},
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := pool.Get().([]byte)
			defer pool.Put(buf)

			// Use the buffer
			copy(buf, "hello")
			fmt.Println("Buffer content:", string(buf[:5]))
		}()
	}
	wg.Wait()
}

// 11. Error Handling in Goroutines
func errorHandlingInGoroutines() {
	type Result struct {
		Value int
		Error error
	}

	worker := func(id int, resultChan chan<- Result) {
		if id%2 == 0 {
			resultChan <- Result{Value: id * 10, Error: nil}
		} else {
			resultChan <- Result{Error: fmt.Errorf("error from worker %d", id)}
		}
	}

	resultChan := make(chan Result, 5)
	for i := 0; i < 5; i++ {
		go worker(i, resultChan)
	}

	for i := 0; i < 5; i++ {
		result := <-resultChan
		if result.Error != nil {
			fmt.Printf("Error: %v\n", result.Error)
		} else {
			fmt.Printf("Result: %d\n", result.Value)
		}
	}
}

// 12. Rate Limiting Pattern
func rateLimiting() {
	requests := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		requests <- i
	}
	close(requests)

	limiter := time.Tick(200 * time.Millisecond)

	for req := range requests {
		<-limiter
		fmt.Println("Request", req, time.Now())
	}

	burstyLimiter := make(chan time.Time, 3)
	for i := 0; i < 3; i++ {
		burstyLimiter <- time.Now()
	}

	go func() {
		for t := range time.Tick(200 * time.Millisecond) {
			burstyLimiter <- t
		}
	}()

	burstyRequests := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		burstyRequests <- i
	}
	close(burstyRequests)

	for req := range burstyRequests {
		<-burstyLimiter
		fmt.Println("Bursty request", req, time.Now())
	}
}

// 13. Circuit Breaker Pattern
type CircuitBreaker struct {
	failures     int
	maxFailures  int
	resetTimeout time.Duration
	lastFailure  time.Time
	mu           sync.Mutex
}

func (cb *CircuitBreaker) Execute(action func() error) error {
	cb.mu.Lock()

	if cb.failures >= cb.maxFailures {
		if time.Since(cb.lastFailure) > cb.resetTimeout {
			cb.failures = 0
		} else {
			cb.mu.Unlock()
			return errors.New("circuit breaker is open")
		}
	}
	cb.mu.Unlock()

	err := action()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.failures++
		cb.lastFailure = time.Now()
		return fmt.Errorf("action failed: %w (failures: %d)", err, cb.failures)
	}

	if cb.failures > 0 {
		cb.failures--
	}
	return nil
}

func circuitBreakerPattern() {
	cb := &CircuitBreaker{
		maxFailures:  2,
		resetTimeout: 5 * time.Second,
	}

	for i := 0; i < 10; i++ {
		err := cb.Execute(func() error {
			if rand.Intn(2) == 0 {
				return errors.New("random failure")
			}
			fmt.Println("Action succeeded")
			return nil
		})

		if err != nil {
			fmt.Println("Error:", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// 14. Pub/Sub Pattern
type PubSub struct {
	subscribers map[string][]chan string
	mu          sync.RWMutex
}

func NewPubSub() *PubSub {
	return &PubSub{
		subscribers: make(map[string][]chan string),
	}
}

func (ps *PubSub) Subscribe(topic string) <-chan string {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ch := make(chan string, 1)
	ps.subscribers[topic] = append(ps.subscribers[topic], ch)
	return ch
}

func (ps *PubSub) Publish(topic string, message string) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	for _, ch := range ps.subscribers[topic] {
		go func(ch chan string) {
			ch <- message
		}(ch)
	}
}

func pubSubPattern() {
	ps := NewPubSub()

	sub1 := ps.Subscribe("news")
	sub2 := ps.Subscribe("sports")

	go func() {
		for msg := range sub1 {
			fmt.Println("Sub1 received:", msg)
		}
	}()

	go func() {
		for msg := range sub2 {
			fmt.Println("Sub2 received:", msg)
		}
	}()

	ps.Publish("news", "Breaking news!")
	ps.Publish("sports", "Goal scored!")
	time.Sleep(100 * time.Millisecond)
}

// 15. Load Balancer Pattern
type Worker struct {
	requests chan<- int
	results  <-chan int
}

func NewWorker() *Worker {
	requests := make(chan int)
	results := make(chan int)

	go func() {
		for req := range requests {
			time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
			results <- req * 2
		}
	}()

	return &Worker{requests: requests, results: results}
}

type LoadBalancer struct {
	workers []*Worker
	next    int
}

func NewLoadBalancer(numWorkers int) *LoadBalancer {
	lb := &LoadBalancer{}
	for i := 0; i < numWorkers; i++ {
		lb.workers = append(lb.workers, NewWorker())
	}
	return lb
}

func (lb *LoadBalancer) Dispatch(request int) <-chan int {
	worker := lb.workers[lb.next%len(lb.workers)]
	lb.next++
	worker.requests <- request
	return worker.results
}

func loadBalancerPattern() {
	lb := NewLoadBalancer(3)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			result := <-lb.Dispatch(i)
			fmt.Printf("Request %d -> Result %d\n", i, result)
		}(i)
	}
	wg.Wait()
}

func main() {
	basicConcurrency()
	channelPatterns()
	workerPool()
	fanOutFanIn()
	pipelinePattern()
	contextPattern()
	mutexPatterns()
	rwMutexPattern()
	oncePattern()
	poolPattern()
	errorHandlingInGoroutines()
	rateLimiting()
	circuitBreakerPattern()
	pubSubPattern()
	loadBalancerPattern()
}
