package encryption

import (
	"testing"
)

func TestEncryptDecrypt(t *testing.T) {
	key := []byte("12345678901234567890123456789012") // 32 bytes for AES-256
	text := "hello world"

	encrypted, err := Encrypt(key, text)
	if err != nil {
		t.Fatalf("encrypt failed: %v", err)
	}

	decrypted, err := Decrypt(key, encrypted)
	if err != nil {
		t.Fatalf("decrypt failed: %v", err)
	}

	if decrypted != text {
		t.Errorf("expected %q, got %q", text, decrypted)
	}
}

func TestEncryptDecryptSmallKey(t *testing.T) {
	key := []byte("1234567890123456") // 16 bytes for AES-128
	text := "hello world"

	encrypted, err := Encrypt(key, text)
	if err != nil {
		t.Fatalf("encrypt failed: %v", err)
	}

	decrypted, err := Decrypt(key, encrypted)
	if err != nil {
		t.Fatalf("decrypt failed: %v", err)
	}

	if decrypted != text {
		t.Errorf("expected %q, got %q", text, decrypted)
	}
}
