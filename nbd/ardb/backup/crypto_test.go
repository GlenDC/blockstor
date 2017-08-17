package backup

import (
	"bytes"
	"crypto/rand"
	mrand "math/rand"
	"testing"
)

func TestCryptoAES(t *testing.T) {
	encrypter, err := NewEncrypter(&privKey)
	if err != nil {
		t.Fatal(err)
	}
	decrypter, err := NewDecrypter(&privKey)
	if err != nil {
		t.Fatal(err)
	}

	testCrypto(t, encrypter, decrypter)
}

func testCrypto(t *testing.T, encrypter Encrypter, decrypter Decrypter) {
	randTestCase := make([]byte, 4*1024)
	rand.Read(randTestCase)

	testCases := [][]byte{
		make([]byte, 4*1024),
		[]byte("This is a testcase."),
		[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0},
		randTestCase,
	}

	hlrounds := (mrand.Int() % 4) + 3

	var bufA, bufB bytes.Buffer

	for _, original := range testCases {
		var err error

		// encrypt `hlround` times
		bufA.Reset()
		bufA.Write(original)
		for i := 0; i < hlrounds; i++ {
			err = encrypter.Encrypt(&bufA, &bufB)
			if err != nil {
				t.Error(err)
				break
			}
			bufA.Reset()
			_, err = bufA.ReadFrom(&bufB)
			if err != nil {
				t.Error(err)
				break
			}
			bufB.Reset()
		}
		if err != nil {
			err = nil
			continue
		}
		// decrypt `hlround` times
		for i := 0; i < hlrounds; i++ {
			err = decrypter.Decrypt(&bufA, &bufB)
			if err != nil {
				t.Error(err)
				break
			}
			bufA.Reset()
			_, err = bufA.ReadFrom(&bufB)
			if err != nil {
				t.Error(err)
				break
			}
			bufB.Reset()
		}
		if err != nil {
			err = nil
			continue
		}

		plain := bufA.Bytes()

		if bytes.Compare(original, plain) != 0 {
			t.Errorf(
				"plaintext expected to be %v, while received %v",
				original, plain)
		}
	}
}

func BenchmarkAES_4k(b *testing.B) {
	benchmarkAES(b, 4*1024)
}

func BenchmarkAES_8k(b *testing.B) {
	benchmarkAES(b, 8*1024)
}

func BenchmarkAES_16k(b *testing.B) {
	benchmarkAES(b, 16*1024)
}

func BenchmarkAES_32k(b *testing.B) {
	benchmarkAES(b, 32*1024)
}

func benchmarkAES(b *testing.B, size int64) {
	encrypter, err := NewEncrypter(&privKey)
	if err != nil {
		b.Fatal(err)
	}
	decrypter, err := NewDecrypter(&privKey)
	if err != nil {
		b.Fatal(err)
	}

	benchmarkCrypto(b, size, encrypter, decrypter)
}

func benchmarkCrypto(b *testing.B, size int64, encrypter Encrypter, decrypter Decrypter) {
	in := make([]byte, size)
	b.SetBytes(size)

	for i := 0; i < b.N; i++ {
		var err error
		var bufA, bufB bytes.Buffer

		_, err = bufA.Write(in)
		if err != nil {
			b.Error(err)
			continue
		}

		err = encrypter.Encrypt(&bufA, &bufB)
		if err != nil {
			b.Error(err)
			continue
		}

		bufA.Reset()
		err = decrypter.Decrypt(&bufB, &bufA)
		if err != nil {
			b.Error(err)
			continue
		}

		result := bufA.Bytes()
		if bytes.Compare(in, result) != 0 {
			b.Errorf(
				"decrypted package was expected to be %v, while received %v",
				in, result)
			continue
		}
	}
}

var (
	privKey CryptoKey
)

func init() {
	rand.Read(privKey[:])
}