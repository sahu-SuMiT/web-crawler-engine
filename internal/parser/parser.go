package parser

import (
	"bytes"
	"net/url"
	"strings"

	"golang.org/x/net/html"
)

// HTMLParser handles link extraction and URL canonicalization.
type HTMLParser struct{}

// NewHTMLParser creates a new HTMLParser instance.
func NewHTMLParser() *HTMLParser {
	return &HTMLParser{}
}

// ExtractLinks parses HTML body bytes and returns a list of canonical outbound URLs.
func (p *HTMLParser) ExtractLinks(rawHTML []byte, baseURLStr string) ([]string, error) {
	baseURL, err := url.Parse(baseURLStr)
	if err != nil {
		return nil, err
	}

	tokenizer := html.NewTokenizer(bytes.NewReader(rawHTML))
	linksMap := make(map[string]struct{})

	for {
		tokenType := tokenizer.Next()
		if tokenType == html.ErrorToken {
			break // Reached EOF or parse error
		}

		token := tokenizer.Token()
		if tokenType == html.StartTagToken || tokenType == html.SelfClosingTagToken {
			if token.Data == "a" {
				for _, attr := range token.Attr {
					if attr.Key == "href" {
						canonicalURL, ok := CanonicalizeURL(attr.Val, baseURL)
						if ok {
							linksMap[canonicalURL] = struct{}{}
						}
					}
				}
			}
		}
	}

	extracted := make([]string, 0, len(linksMap))
	for link := range linksMap {
		extracted = append(extracted, link)
	}

	return extracted, nil
}

// CanonicalizeURL normalizes raw link references relative to a base URL.
// Strips anchor fragments, standardizes scheme, and filters out non-HTTP schemes (mailto, javascript, tel).
func CanonicalizeURL(rawHref string, baseURL *url.URL) (string, bool) {
	rawHref = strings.TrimSpace(rawHref)
	if rawHref == "" || strings.HasPrefix(rawHref, "javascript:") || strings.HasPrefix(rawHref, "mailto:") || strings.HasPrefix(rawHref, "tel:") || strings.HasPrefix(rawHref, "data:") {
		return "", false
	}

	ref, err := url.Parse(rawHref)
	if err != nil {
		return "", false
	}

	resolved := baseURL.ResolveReference(ref)

	// Filter non-http/https
	scheme := strings.ToLower(resolved.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", false
	}

	// Clean fields
	resolved.Scheme = scheme
	resolved.Host = strings.ToLower(resolved.Host)
	resolved.Fragment = "" // Strip anchor tag fragments

	// Remove default ports
	if (scheme == "http" && resolved.Port() == "80") || (scheme == "https" && resolved.Port() == "443") {
		resolved.Host = resolved.Hostname()
	}

	finalURL := resolved.String()
	return finalURL, true
}
