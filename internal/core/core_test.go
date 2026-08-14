package core

import (
	"errors"
	"fmt"
	"testing"
)

// IsNotFound matches on the upstream client's error message because it
// exposes no typed error; this pins the contract so a silent upstream
// message change fails loudly here instead of in production behavior.
func TestIsNotFound(t *testing.T) {
	notFound := fmt.Errorf("failed to get resource cckv://con1u/x: status code 404")
	if !IsNotFound(notFound) {
		t.Fatal("expected 404 error to match")
	}
	if !IsNotFound(errors.Join(errors.New("failed to get signed document"), notFound)) {
		t.Fatal("expected wrapped 404 error to match")
	}
	if IsNotFound(fmt.Errorf("failed to get resource cckv://con1u/x: status code 500")) {
		t.Fatal("500 must not match")
	}
	if IsNotFound(nil) {
		t.Fatal("nil must not match")
	}
}
