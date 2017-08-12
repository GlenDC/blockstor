package backup

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
	"io/ioutil"
)

// static sizes
const (
	CryptoKeySize = 32 // 256-bit key
)

// Encrypt a given plain text using
// AES256 in Galois Counter Mode, with a random nonce.
func Encrypt(key *CryptoKey, src io.Reader, dst io.Writer) error {
	encrypter, err := NewEncrypter(key)
	if err != nil {
		return err
	}
	return encrypter.Encrypt(src, dst)
}

// Decrypt a given cipher text,
// previously encrypted using AES256 in Galois Counter Mode.
func Decrypt(key *CryptoKey, src io.Reader, dst io.Writer) error {
	decrypter, err := NewDecrypter(key)
	if err != nil {
		return err
	}
	return decrypter.Decrypt(src, dst)
}

// NewEncrypter creates an object using the given private key,
// which allows you to encrypt plain text using AES256 in Galois Counter Mode.
func NewEncrypter(key *CryptoKey) (Encrypter, error) {
	return newAESSTDStreamCipher(key)
}

// NewDecrypter creates an object using the given private key,
// which allows you to decrypt cipher text,
// which was previously encrypted using AES256 in Galois Counter Mode.
func NewDecrypter(key *CryptoKey) (Decrypter, error) {
	return newAESSTDStreamCipher(key)
}

// Encrypter defines the API,
// which allows you to encrypt a given plain text into cipher text.
// By default we use AES256 in Galois Counter Mode.
type Encrypter interface {
	Encrypt(src io.Reader, dst io.Writer) error
}

// Decrypter defines the API,
// which allows you to decrypt a given cipher text,
// which was previously encrypted by the Encrypter which
// acts as the counterpart of this interface.
type Decrypter interface {
	Decrypt(src io.Reader, dst io.Writer) error
}

// create a new AES256 encrypter/decrypter in Galois Counter Mode.
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
func (s *aesSTDStreamCipher) Encrypt(src io.Reader, dst io.Writer) error {
	plain, err := ioutil.ReadAll(src)
	if err != nil {
		return err
	}

	nonce := make([]byte, s.aesgcm.NonceSize())
	_, err = io.ReadFull(rand.Reader, nonce)
	if err != nil {
		return err
	}

	cipher := s.aesgcm.Seal(nonce, nonce, plain, nil)
	_, err = dst.Write(cipher)
	return err
}

// Decrypt implements Decrypter.Decrypt
func (s *aesSTDStreamCipher) Decrypt(src io.Reader, dst io.Writer) error {
	cipher, err := ioutil.ReadAll(src)
	if err != nil {
		return err
	}

	nonceSize := s.aesgcm.NonceSize()
	if len(cipher) < nonceSize {
		return errors.New("malformed ciphertext")
	}

	plain, err := s.aesgcm.Open(nil, cipher[:nonceSize], cipher[nonceSize:], nil)
	if err != nil {
		return err
	}

	_, err = dst.Write(plain)
	return err
}

// CryptoKey defines the type of a CryptoKey
type CryptoKey [CryptoKeySize]byte

// String implements Value.String
func (key CryptoKey) String() string {
	return string(key[:])
}

// Set implements Value.Set
func (key CryptoKey) Set(value string) error {
	if len(value) != CryptoKeySize {
		return errors.New("wrong crypto key size")
	}

	copy(key[:], value)
	return nil
}

// Type implements PValue.Type
func (key CryptoKey) Type() string {
	return "AESCryptoKey"
}
