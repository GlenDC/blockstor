package backup

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

// static sizes
const (
	CryptoKeySize = 32 // 256-bit key
)

func Encrypt(key *CryptoKey, plain []byte) (cipher []byte, err error) {
	encrypter, err := NewEncrypter(key)
	if err != nil {
		return
	}

	cipher, err = encrypter.Encrypt(plain)
	return
}

func Decrypt(key *CryptoKey, cipher []byte) (plain []byte, err error) {
	decrypter, err := NewDecrypter(key)
	if err != nil {
		return
	}

	plain, err = decrypter.Decrypt(plain)
	return
}

func NewEncrypter(key *CryptoKey) (Encrypter, error) {
	return newAESSTDStreamCipher(key)
}

func NewDecrypter(key *CryptoKey) (Decrypter, error) {
	return newAESSTDStreamCipher(key)
}

type Encrypter interface {
	Encrypt(plain []byte) (cipher []byte, err error)
}

type Decrypter interface {
	Decrypt(cipher []byte) (plain []byte, err error)
}

func newAESSTDStreamCipher(key *CryptoKey) (stream *aesSTDStreamCipher, err error) {
	if key == nil {
		err = errors.New("no private key given")
		return
	}

	block, err := aes.NewCipher(key[:])
	if err != nil {
		return
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return
	}

	stream = &aesSTDStreamCipher{aesgcm}
	return
}

type aesSTDStreamCipher struct {
	aesgcm cipher.AEAD
}

// Encrypt implements Encrypter.Encrypt
func (s *aesSTDStreamCipher) Encrypt(plain []byte) ([]byte, error) {
	nonce := make([]byte, s.aesgcm.NonceSize())
	_, err := io.ReadFull(rand.Reader, nonce)
	if err != nil {
		return nil, err
	}

	cipher := s.aesgcm.Seal(nonce, nonce, plain, nil)
	return cipher, nil
}

// Decrypt implements Decrypter.Decrypt
func (s *aesSTDStreamCipher) Decrypt(cipher []byte) ([]byte, error) {
	nonceSize := s.aesgcm.NonceSize()
	if len(cipher) < nonceSize {
		return nil, errors.New("malformed ciphertext")
	}

	return s.aesgcm.Open(nil, cipher[:nonceSize], cipher[nonceSize:], nil)
}

type CryptoKey [CryptoKeySize]byte
