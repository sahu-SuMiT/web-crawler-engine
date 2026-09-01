package politeness

import (
	"testing"

	"github.com/temoto/robotstxt"
)

func TestRobotsTxtParsing(t *testing.T) {
	robotsContent := `
User-agent: *
Disallow: /admin/
Disallow: /private/
Crawl-delay: 2
`

	data, err := robotstxt.FromStatusAndBytes(200, []byte(robotsContent))
	if err != nil {
		t.Fatalf("Failed to parse robots.txt content: %v", err)
	}

	group := data.FindGroup("SOTACrawler")
	if group == nil {
		t.Fatalf("Expected matching group for SOTACrawler")
	}

	if group.Test("/admin/dashboard") {
		t.Errorf("Expected /admin/dashboard to be disallowed")
	}

	if !group.Test("/blog/article") {
		t.Errorf("Expected /blog/article to be allowed")
	}
}
