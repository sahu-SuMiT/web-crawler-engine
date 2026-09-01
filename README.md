# Web Crawler Go-Engine

An event-driven web crawler, designed to manage large crawl frontiers with concurrency, persistence, host-level politeness, and content archival.

The system combines a **Pebble-backed persistent frontier**, concurrent **fasthttp** workers, streaming HTML parsing, URL deduplication, **WARC** archival, and crawler observability into a decoupled crawling pipeline.

![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=for-the-badge&logo=go)
![Architecture](https://img.shields.io/badge/Architecture-Event--Driven-FF6F00?style=for-the-badge)
![Storage](https://img.shields.io/badge/Storage-Pebble%20%7C%20R2%20%7C%20Neon-4169E1?style=for-the-badge)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

---

## 📐 Architecture

The crawler follows an event-driven pipeline that separates **frontier management, request scheduling, fetching, parsing, and persistence**.

![System Architecture](architecture.png)


## ✨ Key Features

- **Concurrent HTTP Fetching:** `valyala/fasthttp` to support concurrent HTTP retrieval and connection pooling.

- **URL Frontier:** **Pebble DB** for an embedded LSM-tree store to persist crawl state and support disk-backed frontier management.

- **URL Deduplication:** In-memory **Bloom Filter** (`bitsandblooms/bloom`) to perform membership checks before persistent lookups.

- **Host Politeness:** Token-bucket rate limiter (`golang.org/x/time/rate`) to control request frequency for each host.

- **WARC Archival:** Converts crawled responses into `.warc.gz` archives for durable web-content storage.

- **Live Observability:** Embeds web control center using Go `embed` and Server-Sent Events (SSE) for live crawler monitoring.

- **Cloud Persistence:** Cloudflare R2 for WARC archives and PostgreSQL for crawl metadata.

## 🛠️ Technology Stack

| Layer | Technology |
|---|---|
| **Runtime** | **Go 1.22+** |
| **Frontier** | **Pebble DB** |
| **HTTP** | **fasthttp** |
| **Deduplication** | **Bloom Filter** |
| **Politeness** | **`x/time/rate`** |
| **Archival** | **WARC + Cloudflare R2** |
| **Metadata** | **PostgreSQL** |
| **Observability** | **Go `embed` + SSE** |

## 📂 Project Directory Structure

```text
sota-crawler/
├── cmd/
│   └── crawler/
│       └── main.go
├── internal/
│   ├── domain/
│   ├── frontier/
│   ├── fetcher/
│   ├── politeness/
│   ├── parser/
│   ├── storage/
│   └── web/
├── data/
├── go.mod
└── README.md

```

## 🌱 Getting Started

### Prerequisites
* **Go 1.22+**

### Installation

**Clone the repository**
```bash
git clone [https://github.com/sahu-SuMiT/web-crawler-engine.git](https://github.com/sahu-SuMiT/web-crawler-engine.git)
cd web-crawler-engine
```

**Download dependencies**
```bash
go mod download
```

**Run the crawler**
```bash
go run cmd/crawler/main.go \
  -seed="[https://websiteToScrap.com](https://websiteToScrap.com)" \
  -depth=3 \
  -workers=10
```

**Open the dashboard**
Open `http://localhost:8080` in your browser to monitor the crawler.

---

## 📄 License

This project is licensed under the MIT License.
