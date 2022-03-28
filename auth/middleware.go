package auth

import (
	"context"
	"fmt"
	"log"
	"net/http"
)

var UserCtxKey = &contextKey{"user"}

type contextKey struct {
	name string
}

type User struct {
	ID             int
	Name           string
	LoginID        string
	Status         int
	IsSub          bool
	Currency       string
	Permission     int64
	PasswordExpiry int64
	LastLogin      int64
	LastLoginFrom  string
	Type           int
	ParentID       int64
	Profiles       map[string]interface{}
	Roles          []string
}

func Middleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token := r.Header.Get("Authorization")

			// Allow unauthenticated users in
			if token == "" {
				next.ServeHTTP(w, r)
				return
			}

			fmt.Printf("Token: %v\n", token)

			// userId, err := validateAndGetUserID(c)
			// if err != nil {
			// 	http.Error(w, "Invalid cookie", http.StatusForbidden)
			// 	return
			// }

			// // get the user from the database
			// user := getUserByID(db, userId)

			user := &User{
				ID: 3,
			}
			log.Printf("user: %v", user)
			// put it in context
			ctx := context.WithValue(r.Context(), UserCtxKey, user)

			r = r.WithContext(ctx)
			next.ServeHTTP(w, r)
		})
	}
}

// ForContext finds the user from the context. REQUIRES Middleware to have run.
func ForContext(ctx context.Context) *User {
	fmt.Println(ctx.)
	raw, _ := ctx.Value(UserCtxKey).(*User)
	fmt.Println(raw)
	return raw
}
