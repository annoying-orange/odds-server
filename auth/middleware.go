package auth

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
)

var UserCtxKey = &contextKey{"USER"}

type contextKey struct {
	name string
}

type User struct {
	ID            int                    `json:"userId"`
	Name          string                 `json:"username"`
	LoginID       string                 `json:"loginId"`
	Status        int                    `json:"status"`
	Currency      string                 `json:"currency"`
	Permission    int64                  `json:"permission"`
	LastLogin     int64                  `json:"lastLogin"`
	LastLoginFrom string                 `json:"lastLoginFrom"`
	Type          int                    `json:"level"`
	Profiles      map[string]interface{} `json:"profiles"`
	AccessToken   string                 `json:"accessToken"`
}

func ValidateAndGetUser(token string) (*User, error) {
	method := "GET"
	url := "https://api.wesport88.com/oauth"

	cl := &http.Client{}
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Authorization", token)

	log.Printf("%s '%s' --header 'Authorization: %s'\n", method, url, token)

	res, err := cl.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	log.Println(string(body))

	var user *User

	if err = json.Unmarshal(body, &user); err != nil {
		return nil, err
	}

	return user, nil
}

func Middleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// token := r.Header.Get("Authorization")

			// // Allow unauthenticated users in
			// if token == "" {
			// 	next.ServeHTTP(w, r)
			// 	return
			// }

			// fmt.Printf("Token: %v\n", token)

			// user, err := ValidateAndGetUser(token)
			// if err != nil {
			// 	http.Error(w, "Invalid cookie", http.StatusForbidden)
			// 	return
			// }

			// // // get the user from the database
			// // user := getUserByID(db, userId)

			// log.Printf("user: %v", user)
			// // put it in context
			// ctx := context.WithValue(r.Context(), UserCtxKey, user)

			// r = r.WithContext(ctx)
			next.ServeHTTP(w, r)
		})
	}
}

// ForContext finds the user from the context. REQUIRES Middleware to have run.
func ForContext(ctx context.Context) *User {
	raw, _ := ctx.Value(UserCtxKey).(*User)
	return raw
}
