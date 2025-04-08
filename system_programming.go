// system_programming.go
// Comprehensive System Programming in Go (500 lines)

package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// 1. File Operations
func fileOperations() {
	// Create file
	file, err := os.Create("test.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Write to file
	_, err = file.WriteString("Hello, World!\n")
	if err != nil {
		log.Fatal(err)
	}

	// Read file
	data, err := os.ReadFile("test.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("File content: %s", data)

	// File info
	info, err := os.Stat("test.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("File size: %d bytes\n", info.Size())
	fmt.Printf("Modified: %v\n", info.ModTime())

	// Remove file
	err = os.Remove("test.txt")
	if err != nil {
		log.Fatal(err)
	}
}

// 2. Directory Operations
func directoryOperations() {
	// Create directory
	err := os.Mkdir("testdir", 0755)
	if err != nil {
		log.Fatal(err)
	}

	// Create nested directories
	err = os.MkdirAll("testdir/nested/sub", 0755)
	if err != nil {
		log.Fatal(err)
	}

	// Walk directory
	err = filepath.Walk("testdir", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		fmt.Println(path)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Remove all
	err = os.RemoveAll("testdir")
	if err != nil {
		log.Fatal(err)
	}
}

// 3. Command Execution
func commandExecution() {
	// Simple command
	out, err := exec.Command("ls", "-l").Output()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("LS output:\n%s\n", out)

	// Command with arguments
	cmd := exec.Command("echo", "Hello", "World")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Echo output: %s\n", stdout.String())

	// Command pipeline
	ls := exec.Command("ls")
	grep := exec.Command("grep", "go")

	// Connect ls output to grep input
	pipe, err := ls.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	grep.Stdin = pipe

	// Get grep output
	var grepOut bytes.Buffer
	grep.Stdout = &grepOut

	// Start commands
	err = ls.Start()
	if err != nil {
		log.Fatal(err)
	}
	err = grep.Start()
	if err != nil {
		log.Fatal(err)
	}

	// Wait for commands to finish
	err = ls.Wait()
	if err != nil {
		log.Fatal(err)
	}
	err = grep.Wait()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Pipeline output: %s\n", grepOut.String())
}

// 4. Signal Handling
func signalHandling() {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	// Register signals
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Goroutine to handle signals
	go func() {
		sig := <-sigs
		fmt.Printf("\nReceived signal: %v\n", sig)
		done <- true
	}()

	fmt.Println("Waiting for signal (Ctrl+C to send SIGINT)")
	<-done
	fmt.Println("Exiting gracefully")
}

// 5. Process Management
func processManagement() {
	// Get current process
	pid := os.Getpid()
	fmt.Printf("Process ID: %d\n", pid)

	// Get parent process
	ppid := os.Getppid()
	fmt.Printf("Parent process ID: %d\n", ppid)

	// Find process by PID
	process, err := os.FindProcess(pid)
	if err != nil {
		log.Fatal(err)
	}

	// Kill process (don't actually kill it in this example)
	// err = process.Kill()
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// Get process attributes
	uid := os.Getuid()
	gid := os.Getgid()
	euid := os.Geteuid()
	egid := os.Getegid()
	fmt.Printf("UID: %d, GID: %d, EUID: %d, EGID: %d\n", uid, gid, euid, egid)
}

// 6. File Watching
func fileWatching() {
	// Simple polling-based watcher
	file := "watch.txt"
	lastMod := time.Now()

	// Create file to watch
	f, err := os.Create(file)
	if err != nil {
		log.Fatal(err)
	}
	f.Close()

	fmt.Printf("Watching %s for changes...\n", file)

	for i := 0; i < 5; i++ {
		info, err := os.Stat(file)
		if err != nil {
			log.Fatal(err)
		}

		if info.ModTime().After(lastMod) {
			fmt.Printf("File modified at %v\n", info.ModTime())
			lastMod = info.ModTime()
		}

		// Simulate file change
		if i == 2 {
			f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatal(err)
			}
			_, err = f.WriteString(fmt.Sprintf("Change %d\n", i))
			f.Close()
			if err != nil {
				log.Fatal(err)
			}
		}

		time.Sleep(1 * time.Second)
	}

	os.Remove(file)
}

// 7. File Locking
func fileLocking() {
	file := "lock.txt"
	f, err := os.Create(file)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		f.Close()
		os.Remove(file)
	}()

	// Try to get exclusive lock
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		log.Fatal("Unable to get lock:", err)
	}
	fmt.Println("Acquired exclusive lock")

	// Release lock
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	if err != nil {
		log.Fatal("Unable to release lock:", err)
	}
	fmt.Println("Released lock")
}

// 8. Checksum Calculation
func checksumCalculation() {
	file := "checksum.txt"
	data := []byte("Hello, checksum!")

	// Create test file
	err := os.WriteFile(file, data, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(file)

	// Calculate MD5
	hasher := md5.New()
	_, err = hasher.Write(data)
	if err != nil {
		log.Fatal(err)
	}
	hash := hex.EncodeToString(hasher.Sum(nil))
	fmt.Printf("MD5: %s\n", hash)

	// Calculate file checksum
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	fileHasher := md5.New()
	_, err = io.Copy(fileHasher, f)
	if err != nil {
		log.Fatal(err)
	}
	fileHash := hex.EncodeToString(fileHasher.Sum(nil))
	fmt.Printf("File MD5: %s\n", fileHash)
}

// 9. TCP Server/Client
func tcpCommunication() {
	// Start server
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	// Server goroutine
	go func() {
		defer wg.Done()
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		defer conn.Close()

		message := "Hello from server!"
		conn.Write([]byte(message))
	}()

	// Client goroutine
	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond) // Wait for server to start

		conn, err := net.Dial("tcp", "localhost:8080")
		if err != nil {
			log.Fatal(err)
		}
		defer conn.Close()

		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Client received: %s\n", string(buf[:n]))
	}()

	wg.Wait()
}

// 10. Environment Variables
func environmentVariables() {
	// Set environment variable
	err := os.Setenv("TEST_VAR", "test_value")
	if err != nil {
		log.Fatal(err)
	}

	// Get environment variable
	value := os.Getenv("TEST_VAR")
	fmt.Printf("TEST_VAR=%s\n", value)

	// Get all environment variables
	fmt.Println("All environment variables:")
	for _, env := range os.Environ() {
		fmt.Println(env)
	}

	// Expand variables in string
	path := "Path: $PATH"
	expanded := os.ExpandEnv(path)
	fmt.Println(expanded)
}

// 11. Command-Line Arguments
func commandLineArguments() {
	// Basic arguments
	args := os.Args
	fmt.Println("Command-line arguments:")
	for i, arg := range args {
		fmt.Printf("%d: %s\n", i, arg)
	}

	// Flag package
	var (
		port    int
		verbose bool
		name    string
	)

	flag.IntVar(&port, "port", 8080, "port number")
	flag.BoolVar(&verbose, "verbose", false, "verbose output")
	flag.StringVar(&name, "name", "", "user name")
	flag.Parse()

	fmt.Printf("Port: %d, Verbose: %v, Name: %s\n", port, verbose, name)
	fmt.Println("Non-flag arguments:", flag.Args())
}

// 12. System Information
func systemInformation() {
	// Hostname
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Hostname: %s\n", hostname)

	// Current working directory
	wd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Working directory: %s\n", wd)

	// User home directory
	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Home directory: %s\n", home)

	// User config directory
	config, err := os.UserConfigDir()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Config directory: %s\n", config)

	// Temp directory
	temp := os.TempDir()
	fmt.Printf("Temp directory: %s\n", temp)
}

// 13. File Permissions
func filePermissions() {
	file := "permissions.txt"
	err := os.WriteFile(file, []byte("test"), 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(file)

	// Get file permissions
	info, err := os.Stat(file)
	if err != nil {
		log.Fatal(err)
	}
	mode := info.Mode()
	fmt.Printf("Permissions: %s (%04o)\n", mode, mode)

	// Change permissions
	err = os.Chmod(file, 0600)
	if err != nil {
		log.Fatal(err)
	}

	newInfo, err := os.Stat(file)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("New permissions: %s (%04o)\n", newInfo.Mode(), newInfo.Mode())
}

// 14. Context with Timeout
func contextWithTimeout() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Simulate long-running operation
	select {
	case <-time.After(5 * time.Second):
		fmt.Println("Operation completed")
	case <-ctx.Done():
		fmt.Println("Operation canceled:", ctx.Err())
	}
}

// 15. Log Rotation
type RotatingLogger struct {
	filename string
	maxSize  int64
	file     *os.File
	mu       sync.Mutex
}

func NewRotatingLogger(filename string, maxSize int64) *RotatingLogger {
	return &RotatingLogger{
		filename: filename,
		maxSize:  maxSize,
	}
}

func (rl *RotatingLogger) Write(p []byte) (n int, err error) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if rl.file == nil {
		if err := rl.openOrCreate(); err != nil {
			return 0, err
		}
	}

	// Check size and rotate if needed
	info, err := rl.file.Stat()
	if err != nil {
		return 0, err
	}

	if info.Size()+int64(len(p)) > rl.maxSize {
		rl.file.Close()
		if err := os.Rename(rl.filename, rl.filename+".1"); err != nil {
			return 0, err
		}
		if err := rl.openOrCreate(); err != nil {
			return 0, err
		}
	}

	return rl.file.Write(p)
}

func (rl *RotatingLogger) openOrCreate() error {
	file, err := os.OpenFile(rl.filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	rl.file = file
	return nil
}

func (rl *RotatingLogger) Close() error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if rl.file != nil {
		return rl.file.Close()
	}
	return nil
}

func logRotation() {
	logger := NewRotatingLogger("app.log", 1024) // 1KB max size
	defer logger.Close()

	log.SetOutput(logger)

	for i := 0; i < 100; i++ {
		log.Printf("Log entry %d: %s", i, strings.Repeat("x", 100))
		time.Sleep(10 * time.Millisecond)
	}
}

func main() {
	// Uncomment to run specific examples
	fileOperations()
	directoryOperations()
	commandExecution()
	signalHandling()
	processManagement()
	fileWatching()
	fileLocking()
	checksumCalculation()
	tcpCommunication()
	environmentVariables()
	commandLineArguments()
	systemInformation()
	filePermissions()
	contextWithTimeout()
	logRotation()
}
