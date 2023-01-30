package tokenBucket

import (
	"time"
)

type Token struct {
	TokenNumber int
	Timestamp   time.Time
}

type TokenBucket struct {
	TotalTokenNumber int
	NumberOfMinutes  float64
	Tokens           map[string]Token
}

// Prosledjujemo broj zahteva koji zelimo da dozvolimo, i broj minuta (koliko zahteva po minutu)
func NewTokenBucket(totalNumOfTokens int, numberOfMinutes float64) *TokenBucket {
	mapa := make(map[string]Token)
	tb := TokenBucket{totalNumOfTokens, numberOfMinutes, mapa}
	return &tb
}

// Dodajemo zahtev u Token Bucket, ako nema vise dostupnih zahteva taj korisnik, vratice false
func (tb *TokenBucket) AddRequest(user string) bool {
	token, ok := tb.Tokens[user]
	now := time.Now()
	currentTime := now.Unix()
	if ok {

		if float64(currentTime-token.Timestamp.Unix()) >= tb.NumberOfMinutes*60 {
			token.Timestamp = now
			token.TokenNumber = tb.TotalTokenNumber - 1
			tb.Tokens[user] = token
			//fmt.Println("Success")
			return true
		} else {
			if token.TokenNumber == 0 {
				//fmt.Println("You can't make more requests")
				return false
			} else {
				token.TokenNumber -= 1
				tb.Tokens[user] = token
				//fmt.Println("Success")
				return true
			}
		}
	} else {
		tb.Tokens[user] = Token{tb.TotalTokenNumber - 1, now}
		//fmt.Println("Success")
		return true
	}
}
