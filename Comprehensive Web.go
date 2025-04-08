// Comprehensive Web Development Techniques in Go (500 lines)

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/sessions"
	"golang.org/x/crypto/bcrypt"
)

// 1. Basic HTTP Server
func basicHTTPServer() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Welcome to my website!")
	})

	http.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		if name == "" {
			name = "World"
		}
		fmt.Fprintf(w, "Hello, %s!", name)
	})

	fmt.Println("Server running on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// 2. RESTful API with Gorilla Mux
type Product struct {
	ID    int     `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}

var products = []Product{
	{1, "Laptop", 999.99},
	{2, "Phone", 699.99},
	{3, "Tablet", 399.99},
}

func restfulAPI() {
	r := mux.NewRouter()

	// Routes
	r.HandleFunc("/products", getProducts).Methods("GET")
	r.HandleFunc("/products/{id}", getProduct).Methods("GET")
	r.HandleFunc("/products", createProduct).Methods("POST")
	r.HandleFunc("/products/{id}", updateProduct).Methods("PUT")
	r.HandleFunc("/products/{id}", deleteProduct).Methods("DELETE")

	// Middleware
	r.Use(loggingMiddleware)
	r.Use(contentTypeMiddleware)

	fmt.Println("REST API running on :8081")
	log.Fatal(http.ListenAndServe(":8081", r))
}

func getProducts(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(products)
}

func getProduct(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id, _ := strconv.Atoi(params["id"])

	for _, p := range products {
		if p.ID == id {
			json.NewEncoder(w).Encode(p)
			return
		}
	}
	http.NotFound(w, r)
}

func createProduct(w http.ResponseWriter, r *http.Request) {
	var product Product
	_ = json.NewDecoder(r.Body).Decode(&product)
	product.ID = len(products) + 1
	products = append(products, product)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(product)
}

func updateProduct(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id, _ := strconv.Atoi(params["id"])

	var updated Product
	_ = json.NewDecoder(r.Body).Decode(&updated)

	for i, p := range products {
		if p.ID == id {
			products[i] = updated
			json.NewEncoder(w).Encode(updated)
			return
		}
	}
	http.NotFound(w, r)
}

func deleteProduct(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id, _ := strconv.Atoi(params["id"])

	for i, p := range products {
		if p.ID == id {
			products = append(products[:i], products[i+1:]...)
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}
	http.NotFound(w, r)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s %s", r.RemoteAddr, r.Method, r.URL)
		next.ServeHTTP(w, r)
	})
}

func contentTypeMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}

// 3. HTML Templates
func htmlTemplates() {
	tmpl := template.Must(template.ParseFiles("templates/layout.html", "templates/home.html"))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		data := struct {
			Title   string
			Message string
			Time    time.Time
		}{
			Title:   "My Website",
			Message: "Welcome to my Go web app!",
			Time:    time.Now(),
		}
		tmpl.ExecuteTemplate(w, "layout", data)
	})

	fmt.Println("Template server running on :8082")
	log.Fatal(http.ListenAndServe(":8082", nil))
}

// 4. File Uploads
func fileUploads() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprintf(w, `
				<html>
				<body>
					<form action="/upload" method="post" enctype="multipart/form-data">
						<input type="file" name="file">
						<button type="submit">Upload</button>
					</form>
				</body>
				</html>
			`)
		}
	})

	http.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		file, handler, err := r.FormFile("file")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer file.Close()

		dst, err := os.Create(handler.Filename)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer dst.Close()

		if _, err := io.Copy(dst, file); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(w, "File %s uploaded successfully!", handler.Filename)
	})

	fmt.Println("File upload server running on :8083")
	log.Fatal(http.ListenAndServe(":8083", nil))
}

// 5. Authentication with Cookies
var store = sessions.NewCookieStore([]byte("super-secret-key"))

type User struct {
	Username string
	Password string
}

var users = map[string]User{
	"alice": {"alice", "$2a$10$N9qo8uLOickgx2ZMRZoMy.Mrq5HxHJ1DfWQ6QKPE2k4Jh7g4GQUGq"}, // password: "password123"
}

func authWithCookies() {
	r := mux.NewRouter()

	r.HandleFunc("/", homeHandler)
	r.HandleFunc("/login", loginHandler)
	r.HandleFunc("/logout", logoutHandler)
	r.HandleFunc("/register", registerHandler)
	r.HandleFunc("/profile", profileHandler)

	fmt.Println("Auth server running on :8084")
	log.Fatal(http.ListenAndServe(":8084", r))
}

func homeHandler(w http.ResponseWriter, r *http.Request) {
	session, _ := store.Get(r, "session")
	auth, ok := session.Values["authenticated"].(bool)

	if !ok || !auth {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}

	fmt.Fprintf(w, "Welcome to the home page!")
}

func loginHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		fmt.Fprintf(w, `
			<html>
			<body>
				<form action="/login" method="post">
					<input type="text" name="username" placeholder="Username">
					<input type="password" name="password" placeholder="Password">
					<button type="submit">Login</button>
				</form>
			</body>
			</html>
		`)
		return
	}

	username := r.FormValue("username")
	password := r.FormValue("password")

	user, ok := users[username]
	if !ok {
		http.Error(w, "Invalid credentials", http.StatusUnauthorized)
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(password)); err != nil {
		http.Error(w, "Invalid credentials", http.StatusUnauthorized)
		return
	}

	session, _ := store.Get(r, "session")
	session.Values["authenticated"] = true
	session.Values["username"] = username
	session.Save(r, w)

	http.Redirect(w, r, "/profile", http.StatusSeeOther)
}

func logoutHandler(w http.ResponseWriter, r *http.Request) {
	session, _ := store.Get(r, "session")
	session.Values["authenticated"] = false
	session.Save(r, w)
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func registerHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		fmt.Fprintf(w, `
			<html>
			<body>
				<form action="/register" method="post">
					<input type="text" name="username" placeholder="Username">
					<input type="password" name="password" placeholder="Password">
					<button type="submit">Register</button>
				</form>
			</body>
			</html>
		`)
		return
	}

	username := r.FormValue("username")
	password := r.FormValue("password")

	if _, exists := users[username]; exists {
		http.Error(w, "Username already exists", http.StatusBadRequest)
		return
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		http.Error(w, "Registration failed", http.StatusInternalServerError)
		return
	}

	users[username] = User{username, string(hashedPassword)}
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func profileHandler(w http.ResponseWriter, r *http.Request) {
	session, _ := store.Get(r, "session")
	auth, ok := session.Values["authenticated"].(bool)

	if !ok || !auth {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}

	username := session.Values["username"].(string)
	fmt.Fprintf(w, "Welcome to your profile, %s!", username)
}

// 6. WebSockets
func websockets() {
	// Implementation would use gorilla/websocket
	// This is a placeholder for the 500-line constraint
	fmt.Println("WebSocket server would run here")
}

// 7. Middleware Chain
func middlewareChain() {
	r := mux.NewRouter()

	// Sample middleware
	logger := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log.Println("Before:", r.URL.Path)
			next.ServeHTTP(w, r)
			log.Println("After:", r.URL.Path)
		})
	}

	auth := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "secret" {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}

	r.Handle("/public", logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "Public page")
	})))

	r.Handle("/private", auth(logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "Private page")
	}))))

	fmt.Println("Middleware server running on :8085")
	log.Fatal(http.ListenAndServe(":8085", r))
}

// 8. Rate Limiting
func rateLimiting() {
	type visitor struct {
		limiter  *rate.Limiter
		lastSeen time.Time
	}

	var (
		mu       sync.Mutex
		visitors = make(map[string]*visitor)
	)

	go func() {
		for {
			time.Sleep(time.Minute)
			mu.Lock()
			for ip, v := range visitors {
				if time.Since(v.lastSeen) > 3*time.Minute {
					delete(visitors, ip)
				}
			}
			mu.Unlock()
		}
	}()

	getVisitor := func(ip string) *rate.Limiter {
		mu.Lock()
		defer mu.Unlock()

		v, exists := visitors[ip]
		if !exists {
			limiter := rate.NewLimiter(rate.Every(time.Second), 5)
			visitors[ip] = &visitor{limiter, time.Now()}
			return limiter
		}
		v.lastSeen = time.Now()
		return v.limiter
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		limiter := getVisitor(r.RemoteAddr)
		if !limiter.Allow() {
			http.Error(w, "Too many requests", http.StatusTooManyRequests)
			return
		}
		fmt.Fprintf(w, "Hello, you're visitor #%d", len(visitors))
	})

	fmt.Println("Rate limiting server running on :8086")
	log.Fatal(http.ListenAndServe(":8086", nil))
}

// 9. Graceful Shutdown
func gracefulShutdown() {
	r := mux.NewRouter()
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
		fmt.Fprint(w, "Hello from slow endpoint")
	})

	srv := &http.Server{
		Addr:    ":8087",
		Handler: r,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Simulate shutdown after 30 seconds
	time.Sleep(30 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Shutdown error: %v", err)
	}
	log.Println("Server gracefully stopped")
}

// 10. HTTP/2 Server Push
func http2ServerPush() {
	// Implementation requires TLS
	// This is a placeholder for the 500-line constraint
	fmt.Println("HTTP/2 server would run here")
}

func main() {

	basicHTTPServer()
	restfulAPI()
	htmlTemplates()
	fileUploads()
	authWithCookies()
	middlewareChain()
	rateLimiting()
	gracefulShutdown()
}
