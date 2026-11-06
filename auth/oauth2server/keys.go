package oauth2server

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
)

// LoadRSAKey reads a persistent PKCS#1 or PKCS#8 RSA key from a trusted path.
func LoadRSAKey(path string) (*rsa.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	defer clear(data)
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("oauth2server: PEM private key required")
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("oauth2server: RSA private key required")
	}
	return rsaKey, nil
}
