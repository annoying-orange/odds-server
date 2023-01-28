package auth

import (
	"log"
	"time"

	"github.com/dgrijalva/jwt-go"
)

// secret key being used to sign tokens
var (
	SecretKey = []byte("SB-SECRET-KEY")
)

// GenerateToken generates a jwt token and assign a username to it's claims and return it
func GenerateToken(username string) (string, error) {
	token := jwt.New(jwt.SigningMethodHS256)
	/* Create a map to store our claims */
	claims := token.Claims.(jwt.MapClaims)
	/* Set token claims */
	claims["username"] = username
	claims["exp"] = time.Now().Add(time.Hour * 24).Unix()
	tokenString, err := token.SignedString(SecretKey)
	if err != nil {
		log.Fatal("Error in Generating key")
		return "", err
	}
	return tokenString, nil
}

// ParseToken parses a jwt token and returns the username in it's claims
func ParseToken(tokenStr string) (*User, error) {
	token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
		return SecretKey, nil
	})
	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		user := &User{
			ID:      claims["user_id"].(int),
			Name:    claims["user_name"].(string),
			LoginID: "",
			Status:  claims["user_status"].(int),
			// IsSub:          claims["subuser"].(bool),
			Currency:   claims["currency"].(string),
			Permission: int64(claims["permission"].(float64)),
			// PasswordExpiry: claims["password_expiry"].(int64),
			LastLogin:     claims["last_login"].(int64),
			LastLoginFrom: claims["last_login_from"].(string),
			Type:          claims["user_type"].(int),
			// ParentID:       claims["user_parent_id"].(int64),
			Profiles: claims["profiles"].(map[string]interface{}),
			// Roles:          []string{claims["scope"].(string)},
		}
		return user, nil
	} else {
		return nil, err
	}
}
