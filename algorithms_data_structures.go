// algorithms_data_structures.go
// Comprehensive Algorithms and Data Structures in Go (500 lines)

package main

import (
	"container/heap"
	"container/list"
	"container/ring"
	"fmt"
	"sort"
	"strings"
)

// 1. Sorting Algorithms
func sortingAlgorithms() {
	// Built-in sort
	nums := []int{5, 2, 6, 3, 1, 4}
	sort.Ints(nums)
	fmt.Println("Sorted numbers:", nums)

	// Custom sorting
	people := []struct {
		Name string
		Age  int
	}{
		{"Alice", 25},
		{"Bob", 30},
		{"Charlie", 20},
	}
	sort.Slice(people, func(i, j int) bool {
		return people[i].Age < people[j].Age
	})
	fmt.Println("Sorted people by age:", people)

	// Bubble sort
	bubbleSort := func(arr []int) {
		n := len(arr)
		for i := 0; i < n-1; i++ {
			for j := 0; j < n-i-1; j++ {
				if arr[j] > arr[j+1] {
					arr[j], arr[j+1] = arr[j+1], arr[j]
				}
			}
		}
	}

	arr := []int{64, 34, 25, 12, 22, 11, 90}
	bubbleSort(arr)
	fmt.Println("Bubble sorted:", arr)

	// Quick sort
	quickSort := func(arr []int) []int {
		if len(arr) <= 1 {
			return arr
		}

		pivot := arr[0]
		var left, right []int

		for i := 1; i < len(arr); i++ {
			if arr[i] < pivot {
				left = append(left, arr[i])
			} else {
				right = append(right, arr[i])
			}
		}

		left = quickSort(left)
		right = quickSort(right)

		return append(append(left, pivot), right...)
	}

	arr = []int{10, 80, 30, 90, 40, 50, 70}
	sorted := quickSort(arr)
	fmt.Println("Quick sorted:", sorted)
}

// 2. Searching Algorithms
func searchingAlgorithms() {
	// Linear search
	linearSearch := func(arr []int, target int) int {
		for i, v := range arr {
			if v == target {
				return i
			}
		}
		return -1
	}

	arr := []int{10, 20, 30, 40, 50}
	fmt.Println("Linear search 30:", linearSearch(arr, 30))
	fmt.Println("Linear search 60:", linearSearch(arr, 60))

	// Binary search (requires sorted array)
	binarySearch := func(arr []int, target int) int {
		low, high := 0, len(arr)-1

		for low <= high {
			mid := low + (high-low)/2
			if arr[mid] == target {
				return mid
			} else if arr[mid] < target {
				low = mid + 1
			} else {
				high = mid - 1
			}
		}
		return -1
	}

	sortedArr := []int{10, 20, 30, 40, 50}
	fmt.Println("Binary search 30:", binarySearch(sortedArr, 30))
	fmt.Println("Binary search 60:", binarySearch(sortedArr, 60))
}

// 3. Linked List
func linkedList() {
	// Using container/list
	l := list.New()

	// Add elements
	l.PushBack(1)
	l.PushBack(2)
	l.PushFront(0)

	// Iterate
	fmt.Print("Linked list: ")
	for e := l.Front(); e != nil; e = e.Next() {
		fmt.Print(e.Value, " ")
	}
	fmt.Println()

	// Remove element
	for e := l.Front(); e != nil; e = e.Next() {
		if e.Value == 1 {
			l.Remove(e)
			break
		}
	}

	fmt.Print("After removal: ")
	for e := l.Front(); e != nil; e = e.Next() {
		fmt.Print(e.Value, " ")
	}
	fmt.Println()
}

// 4. Stack and Queue
func stackAndQueue() {
	// Stack using slice
	stack := []int{}
	stack = append(stack, 1) // push
	stack = append(stack, 2)
	stack = append(stack, 3)

	// pop
	top := stack[len(stack)-1]
	stack = stack[:len(stack)-1]
	fmt.Println("Stack top:", top)

	// Queue using slice
	queue := []int{}
	queue = append(queue, 1) // enqueue
	queue = append(queue, 2)
	queue = append(queue, 3)

	// dequeue
	front := queue[0]
	queue = queue[1:]
	fmt.Println("Queue front:", front)
}

// 5. Hash Table
func hashTable() {
	// Using built-in map
	m := make(map[string]int)

	// Insert
	m["one"] = 1
	m["two"] = 2
	m["three"] = 3

	// Lookup
	fmt.Println("Value for 'two':", m["two"])

	// Delete
	delete(m, "two")
	fmt.Println("After delete:", m)

	// Check existence
	if val, exists := m["three"]; exists {
		fmt.Println("'three' exists:", val)
	}

	// Iterate
	fmt.Println("All entries:")
	for k, v := range m {
		fmt.Printf("%s: %d\n", k, v)
	}
}

// 6. Binary Tree
type TreeNode struct {
	Value int
	Left  *TreeNode
	Right *TreeNode
}

func binaryTree() {
	// Create tree
	root := &TreeNode{Value: 1}
	root.Left = &TreeNode{Value: 2}
	root.Right = &TreeNode{Value: 3}
	root.Left.Left = &TreeNode{Value: 4}
	root.Left.Right = &TreeNode{Value: 5}

	// Pre-order traversal
	var preOrder func(*TreeNode)
	preOrder = func(node *TreeNode) {
		if node == nil {
			return
		}
		fmt.Print(node.Value, " ")
		preOrder(node.Left)
		preOrder(node.Right)
	}

	fmt.Print("Pre-order: ")
	preOrder(root)
	fmt.Println()

	// In-order traversal
	var inOrder func(*TreeNode)
	inOrder = func(node *TreeNode) {
		if node == nil {
			return
		}
		inOrder(node.Left)
		fmt.Print(node.Value, " ")
		inOrder(node.Right)
	}

	fmt.Print("In-order: ")
	inOrder(root)
	fmt.Println()

	// Post-order traversal
	var postOrder func(*TreeNode)
	postOrder = func(node *TreeNode) {
		if node == nil {
			return
		}
		postOrder(node.Left)
		postOrder(node.Right)
		fmt.Print(node.Value, " ")
	}

	fmt.Print("Post-order: ")
	postOrder(root)
	fmt.Println()
}

// 7. Graph
type Graph struct {
	vertices int
	adjList  map[int][]int
}

func NewGraph(vertices int) *Graph {
	return &Graph{
		vertices: vertices,
		adjList:  make(map[int][]int),
	}
}

func (g *Graph) AddEdge(src, dest int) {
	g.adjList[src] = append(g.adjList[src], dest)
	g.adjList[dest] = append(g.adjList[dest], src) // for undirected graph
}

func (g *Graph) BFS(start int) {
	visited := make(map[int]bool)
	queue := []int{start}
	visited[start] = true

	fmt.Print("BFS: ")
	for len(queue) > 0 {
		vertex := queue[0]
		queue = queue[1:]
		fmt.Print(vertex, " ")

		for _, neighbor := range g.adjList[vertex] {
			if !visited[neighbor] {
				visited[neighbor] = true
				queue = append(queue, neighbor)
			}
		}
	}
	fmt.Println()
}

func (g *Graph) DFS(start int) {
	visited := make(map[int]bool)
	fmt.Print("DFS: ")
	g.dfsUtil(start, visited)
	fmt.Println()
}

func (g *Graph) dfsUtil(vertex int, visited map[int]bool) {
	visited[vertex] = true
	fmt.Print(vertex, " ")

	for _, neighbor := range g.adjList[vertex] {
		if !visited[neighbor] {
			g.dfsUtil(neighbor, visited)
		}
	}
}

func graph() {
	g := NewGraph(5)
	g.AddEdge(0, 1)
	g.AddEdge(0, 2)
	g.AddEdge(1, 3)
	g.AddEdge(2, 4)

	g.BFS(0)
	g.DFS(0)
}

// 8. Priority Queue
type Item struct {
	value    string
	priority int
	index    int
}

type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority // Max-heap
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) update(item *Item, value string, priority int) {
	item.value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}

func priorityQueue() {
	items := map[string]int{
		"banana": 3, "apple": 2, "pear": 4,
	}

	pq := make(PriorityQueue, len(items))
	i := 0
	for value, priority := range items {
		pq[i] = &Item{
			value:    value,
			priority: priority,
			index:    i,
		}
		i++
	}
	heap.Init(&pq)

	// Insert a new item
	item := &Item{
		value:    "orange",
		priority: 1,
	}
	heap.Push(&pq, item)

	// Update priority
	pq.update(item, item.value, 5)

	fmt.Println("Priority queue:")
	for pq.Len() > 0 {
		item := heap.Pop(&pq).(*Item)
		fmt.Printf("%.2d:%s ", item.priority, item.value)
	}
	fmt.Println()
}

// 9. Ring Buffer
func ringBuffer() {
	r := ring.New(5)

	// Initialize ring with values
	for i := 0; i < r.Len(); i++ {
		r.Value = i
		r = r.Next()
	}

	// Print ring
	fmt.Print("Ring buffer: ")
	r.Do(func(x interface{}) {
		fmt.Print(x, " ")
	})
	fmt.Println()

	// Move and set value
	r = r.Move(2)
	r.Value = 10

	fmt.Print("After modification: ")
	r.Do(func(x interface{}) {
		fmt.Print(x, " ")
	})
	fmt.Println()
}

// 10. Trie (Prefix Tree)
type TrieNode struct {
	children map[rune]*TrieNode
	isEnd    bool
}

type Trie struct {
	root *TrieNode
}

func NewTrie() *Trie {
	return &Trie{root: &TrieNode{children: make(map[rune]*TrieNode)}}
}

func (t *Trie) Insert(word string) {
	node := t.root
	for _, ch := range word {
		if _, exists := node.children[ch]; !exists {
			node.children[ch] = &TrieNode{children: make(map[rune]*TrieNode)}
		}
		node = node.children[ch]
	}
	node.isEnd = true
}

func (t *Trie) Search(word string) bool {
	node := t.root
	for _, ch := range word {
		if _, exists := node.children[ch]; !exists {
			return false
		}
		node = node.children[ch]
	}
	return node.isEnd
}

func (t *Trie) StartsWith(prefix string) bool {
	node := t.root
	for _, ch := range prefix {
		if _, exists := node.children[ch]; !exists {
			return false
		}
		node = node.children[ch]
	}
	return true
}

func trie() {
	t := NewTrie()
	t.Insert("apple")
	fmt.Println("Search 'apple':", t.Search("apple"))     // true
	fmt.Println("Search 'app':", t.Search("app"))         // false
	fmt.Println("StartsWith 'app':", t.StartsWith("app")) // true
	t.Insert("app")
	fmt.Println("Search 'app':", t.Search("app")) // true
}

// 11. Dynamic Programming - Fibonacci
func fibonacciDP() {
	fib := func(n int) int {
		if n <= 1 {
			return n
		}

		dp := make([]int, n+1)
		dp[0], dp[1] = 0, 1

		for i := 2; i <= n; i++ {
			dp[i] = dp[i-1] + dp[i-2]
		}

		return dp[n]
	}

	fmt.Println("Fibonacci DP:")
	for i := 0; i < 10; i++ {
		fmt.Printf("%d ", fib(i))
	}
	fmt.Println()
}

// 12. Dynamic Programming - Knapsack
func knapsackDP() {
	type Item struct {
		weight int
		value  int
	}

	knapsack := func(items []Item, capacity int) int {
		n := len(items)
		dp := make([][]int, n+1)
		for i := range dp {
			dp[i] = make([]int, capacity+1)
		}

		for i := 1; i <= n; i++ {
			for w := 1; w <= capacity; w++ {
				if items[i-1].weight <= w {
					dp[i][w] = max(dp[i-1][w], items[i-1].value+dp[i-1][w-items[i-1].weight])
				} else {
					dp[i][w] = dp[i-1][w]
				}
			}
		}

		return dp[n][capacity]
	}

	items := []Item{
		{weight: 2, value: 3},
		{weight: 3, value: 4},
		{weight: 4, value: 5},
		{weight: 5, value: 6},
	}
	capacity := 5

	fmt.Printf("Knapsack max value: %d\n", knapsack(items, capacity))
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// 13. String Algorithms
func stringAlgorithms() {
	// Palindrome check
	isPalindrome := func(s string) bool {
		s = strings.ToLower(strings.ReplaceAll(s, " ", ""))
		for i := 0; i < len(s)/2; i++ {
			if s[i] != s[len(s)-1-i] {
				return false
			}
		}
		return true
	}

	fmt.Println("Is 'madam' a palindrome?", isPalindrome("madam"))
	fmt.Println("Is 'hello' a palindrome?", isPalindrome("hello"))

	// String reversal
	reverse := func(s string) string {
		runes := []rune(s)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes)
	}

	fmt.Println("Reverse 'hello':", reverse("hello"))

	// Anagram check
	areAnagrams := func(s1, s2 string) bool {
		if len(s1) != len(s2) {
			return false
		}
		counts := make(map[rune]int)
		for _, ch := range s1 {
			counts[ch]++
		}
		for _, ch := range s2 {
			counts[ch]--
			if counts[ch] < 0 {
				return false
			}
		}
		return true
	}

	fmt.Println("Are 'listen' and 'silent' anagrams?", areAnagrams("listen", "silent"))
}

// 14. Math Algorithms
func mathAlgorithms() {
	// GCD
	gcd := func(a, b int) int {
		for b != 0 {
			a, b = b, a%b
		}
		return a
	}

	fmt.Println("GCD of 54 and 24:", gcd(54, 24))

	// Prime check
	isPrime := func(n int) bool {
		if n <= 1 {
			return false
		}
		for i := 2; i*i <= n; i++ {
			if n%i == 0 {
				return false
			}
		}
		return true
	}

	fmt.Println("Is 17 prime?", isPrime(17))
	fmt.Println("Is 15 prime?", isPrime(15))

	// Sieve of Eratosthenes
	sieve := func(n int) []int {
		isPrime := make([]bool, n+1)
		for i := 2; i <= n; i++ {
			isPrime[i] = true
		}

		for p := 2; p*p <= n; p++ {
			if isPrime[p] {
				for i := p * p; i <= n; i += p {
					isPrime[i] = false
				}
			}
		}

		var primes []int
		for i := 2; i <= n; i++ {
			if isPrime[i] {
				primes = append(primes, i)
			}
		}
		return primes
	}

	fmt.Println("Primes up to 30:", sieve(30))
}

// 15. Bit Manipulation
func bitManipulation() {
	// Count set bits
	countSetBits := func(n int) int {
		count := 0
		for n > 0 {
			count += n & 1
			n >>= 1
		}
		return count
	}

	fmt.Println("Set bits in 13 (1101):", countSetBits(13))

	// Power of two check
	isPowerOfTwo := func(n int) bool {
		return n > 0 && (n&(n-1)) == 0
	}

	fmt.Println("Is 16 power of two?", isPowerOfTwo(16))
	fmt.Println("Is 15 power of two?", isPowerOfTwo(15))

	// Swap without temp
	a, b := 5, 10
	fmt.Printf("Before swap: a=%d, b=%d\n", a, b)
	a ^= b
	b ^= a
	a ^= b
	fmt.Printf("After swap: a=%d, b=%d\n", a, b)
}

func main() {

	sortingAlgorithms()
	searchingAlgorithms()
	linkedList()
	stackAndQueue()
	hashTable()
	binaryTree()
	graph()
	priorityQueue()
	ringBuffer()
	trie()
	fibonacciDP()
	knapsackDP()
	stringAlgorithms()
	mathAlgorithms()
	bitManipulation()
}
