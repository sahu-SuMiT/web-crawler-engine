package parser

import (
	"net/url"
	"testing"
)

func TestBloomDeduplicator(t *testing.T) {
	bloom := NewBloomDeduplicator(1000, 0.01)

	url1 := "https://example.com/page1"
	url2 := "https://example.com/page2"

	if bloom.MightContain(url1) {
		t.Errorf("expected url1 to not be in bloom filter initially")
	}

	if !bloom.Add(url1) {
		t.Errorf("expected Add(url1) to return true for new url")
	}

	if bloom.Add(url1) {
		t.Errorf("expected Add(url1) to return false for duplicate url")
	}

	if !bloom.MightContain(url1) {
		t.Errorf("expected MightContain(url1) to return true after adding")
	}

	if bloom.MightContain(url2) {
		t.Errorf("expected url2 to not be in bloom filter")
	}
}

func TestCanonicalizeURL(t *testing.T) {
	baseURL, _ := url.Parse("https://example.com/blog/article1")

	tests := []struct {
		input    string
		expected string
		valid    bool
	}{
		{"/about", "https://example.com/about", true},
		{"https://EXAMPLE.COM/page#section", "https://example.com/page", true},
		{"javascript:void(0)", "", false},
		{"mailto:test@example.com", "", false},
		{"../contact", "https://example.com/contact", true},
	}

	for _, tt := range tests {
		got, ok := CanonicalizeURL(tt.input, baseURL)
		if ok != tt.valid {
			t.Errorf("CanonicalizeURL(%q) valid = %v; want %v", tt.input, ok, tt.valid)
		}
		if ok && got != tt.expected {
			t.Errorf("CanonicalizeURL(%q) = %q; want %q", tt.input, got, tt.expected)
		}
	}
}
