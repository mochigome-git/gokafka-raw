package realtime

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"

	"github.com/golang-jwt/jwt/v5"
)

type JWKS struct {
	Keys []JSONWebKey `json:"keys"`
}

type JSONWebKey struct {
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	Alg string `json:"alg"`
	Use string `json:"use"`

	// RSA fields
	N string `json:"n"`
	E string `json:"e"`

	// EC fields
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

var CachedJWKS *JWKS

func FetchJWKS(projectURL string) (*JWKS, error) {
	resp, err := http.Get(projectURL + "/auth/v1/.well-known/jwks.json")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var jwks JWKS
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		return nil, err
	}

	CachedJWKS = &jwks
	return &jwks, nil
}

func (j *JSONWebKey) RSAPublicKey() (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(j.N)
	if err != nil {
		return nil, err
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(j.E)
	if err != nil {
		return nil, err
	}

	e := 0
	for _, b := range eBytes {
		e = e<<8 + int(b)
	}

	pub := &rsa.PublicKey{
		N: new(big.Int).SetBytes(nBytes),
		E: e,
	}
	return pub, nil
}

func (j *JSONWebKey) ECDSAPublicKey() (*ecdsa.PublicKey, error) {
	if j.Kty != "EC" || j.Crv != "P-256" {
		return nil, fmt.Errorf("unsupported EC key type or curve")
	}

	xBytes, err := base64.RawURLEncoding.DecodeString(j.X)
	if err != nil {
		return nil, err
	}
	yBytes, err := base64.RawURLEncoding.DecodeString(j.Y)
	if err != nil {
		return nil, err
	}

	pub := &ecdsa.PublicKey{
		Curve: elliptic.P256(),
		X:     new(big.Int).SetBytes(xBytes),
		Y:     new(big.Int).SetBytes(yBytes),
	}
	return pub, nil
}

func claimString(claims jwt.MapClaims, key string) string {
	if val, ok := claims[key].(string); ok {
		return val
	}
	return "" // or return a default/fallback value
}
