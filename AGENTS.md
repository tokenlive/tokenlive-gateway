# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project Overview

tokenlive-gateway is a high-performance LLM API gateway in Go, inspired by LiteLLM. It provides a unified OpenAI-compatible API that routes requests to multiple LLM providers (OpenAI, Anthropic, Google, DeepSeek, Qwen, Ollama, etc.). The project is written in Chinese (comments, README, commit messages).

## Redis Connection

When using `redis-cli` or other Redis tools for testing/debugging, **DO NOT** use the default `127.0.0.1:6379`. Always use the Redis address configured in `config/local.yml`.

### Quick Connect (Recommended)

Use the provided helper script:

```bash
# Connect to Redis CLI
./scripts/redis-connect.sh

# Execute a single command
./scripts/redis-connect.sh KEYS "aigw:*"

# Use with pipe
echo "INFO memory" | ./scripts/redis-connect.sh
```

### Manual Connection

```bash
# Read Redis config from local.yml
REDIS_ADDR=$(grep -A 5 "redis:" config/local.yml | grep "addr:" | awk '{print $2}')
REDIS_PASSWORD=$(grep -A 5 "redis:" config/local.yml | grep "password:" | awk '{print $2}')

# Connect to Redis with correct config
redis-cli -h $(echo $REDIS_ADDR | cut -d: -f1) -p $(echo $REDIS_ADDR | cut -d: -f2) -a "$REDIS_PASSWORD"
```

### Health Check

Check TTL status of all `aigw:*` keys:

```bash
./scripts/check-redis-ttl.sh
```

Current config (from `config/local.yml`):

## Build & Development Commands

```bash
make init          # Install dev tools: wire, mockgen, swag
make build         # Build binary to ./bin/tokenlive-gateway
make test          # Run tests with coverage (outputs coverage.html)
make mock          # Regenerate gomock mocks
make swag          # Regenerate Swagger docs from annotations in cmd/server/main.go
make bootstrap     # Start docker-compose infra, run migration, start tokenlive-gateway
```

Run the server directly:

```bash
go run ./cmd/server        # Uses config/local.yml by default
APP_CONF=config/prod.yml go run ./cmd/server  # Use prod config
```

Run a single test file or specific test:

```bash
go test ./test/server/handler/... -run TestUserHandler_Register -v
go test ./internal/service/ -run TestRetry -v
```

Run migration: `go run ./cmd/migration`

## Architecture

### Target Architecture (Gin Shell + Engine Pipeline + Invoker Model)

Design doc: `docs/architecture.md` (v2.0, covers 173 architecture decisions)

Gin handles outer plumbing (CORS, Swagger, User routes). LLM traffic enters via a thin Gin handler that dispatches to a custom Engine pipeline with zero Gin dependency.

```
Gin Global Middleware (CORS, Swagger, User routes)
  └─> Gin LLM Handler → engine.HandleRequest(c.Writer, c.Request)
        └─> InboundFilter chain (auth → rate-limit pre-alloc → validate) — execute once
        └─> ClusterInvoker
              ├─ Discovery.List()         → available ProviderInvokers
              ├─ RouterChain.Route()      → filter (circuit breaker, sticky, model)
              ├─ LoadBalancer.Select()     → pick one
              ├─ ProviderInvoker.Invoke()  → actual LLM call + SSE intercept
              └─ retry/failover           → exclude failed, re-select
        └─> OutboundFilter chain (token settlement → metrics → sticky save → log) — execute once
```

Key concepts:

- **Three-layer filter model**: InboundFilter (1x per request) → ClusterInvoker (retryable) → OutboundFilter (1x per request). Retry only re-enters the Invoker, never re-executes Inbound/Outbound.
- **Invoker**: Unified call abstraction. `ProviderInvoker` (leaf, wraps a Provider + Endpoint) and `ClusterInvoker` (orchestrator, ties Discovery + RouterChain + LB + retry).
- **Discovery**: Provides available ProviderInvoker list. Backed by static config or Kubernetes. Reuses `pkg/discovery/`.
- **Router chain**: Each Router is a pure list filter (list-in, list-out). CircuitBreakerRouter, StickySessionRouter, ModelRouter, CostRouter, LatencyRouter.
- **StateStore**: Unified state abstraction (rate limit tokens, session routing, channel latency, circuit breaker status). In-memory or Redis.

### Current Implementation (being migrated)

- **`cmd/`** — Three entry points: `server` (HTTP API), `migration` (DB migrate), `task` (cron jobs). Wire DI in `cmd/server/wire/`.
- **`internal/handler/`** — Gin HTTP handlers. User handlers (`user.go`) and LLM handlers (`llm_handler.go`, `llm_helpers.go`).
- **`internal/service/`** — Business logic. `llm.go` has retry + fallback (will be replaced by ClusterInvoker).
- **`internal/middleware/`** — Gin middleware (will be migrated to InboundFilter/OutboundFilter for LLM routes).
- **`internal/router/`** — Route registration via Wire.
- **`internal/repository/`** — GORM data access (MySQL, PostgreSQL, SQLite).

### LLM Core Library (`pkg/llm/`)

- **`types.go`** — `Provider` interface and OpenAI-compatible request/response types. Will be wrapped by `ProviderInvoker`.
- **`factory.go`** — Provider creation entry point. `NewProvider(type, cfg)` looks up registered factories via `core.GetProviderFactory`.
- **`sse_parser.go`** — Incremental SSE frame parser. `SSEParser.Feed()` buffers partial data and returns complete events.
- **`sse_intercept_writer.go`** — Wraps `http.ResponseWriter` to record TTFT and extract token counts from SSE frames.
- **`router.go`** — Model name → provider mapping. Model resolution logic moves to `ModelRouter` + `StaticDiscovery`.
- **`providers/`** — OpenAI and Anthropic implementations. Each registers via `init()` calling `core.RegisterProviderFactory` and `core.RegisterRequestInvoker` for specific request types, translating between OpenAI format and native API.
- **`config.go`** — Config structs for model mappings, retry policy, rate limits, fallback chains, load balancing.

### Infrastructure Packages (`pkg/`)

- **`discovery/`** — Service discovery (static config and Kubernetes). Backs the Discovery interface.
- **`loadbalancer/`** — Five implemented strategies: round-robin, weighted, random, least-connections, least-latency. Backs the LoadBalancer interface.
- **`health/`** — Health checking for upstream endpoints. Used within Discovery.
- **`governance/`** — Service governance integration. Will be decomposed: Discovery + Health + LB used directly by ClusterInvoker.

## Key Patterns

- **Provider registration**: Providers use `init()` to call `core.RegisterProviderFactory()` and `core.RegisterRequestInvoker()`. Import `_ "tokenlive-gateway/pkg/llm/providers"` to trigger all provider `init()` functions. Use `llm.NewProvider(type, cfg)` to create instances.

- **Wire DI**: All dependency wiring is in `cmd/server/wire/`. Run `wire ./cmd/server/wire/` after changing provider sets.
- **Config**: Viper-based, YAML files in `config/`. The `llm` section defines model_list, providers, retry, rate_limits, fallbacks, and load_balancing. See `config/llm.example.yml` for reference.
- **Testing**: Two patterns coexist — external tests in `test/server/` (user domain, using gomock + httpexpect + sqlmock) and co-located tests in `internal/` (LLM domain, using testify + httptest).

## API Endpoints

- `POST /v1/chat/completions` — Chat completion (stream via `"stream": true`)
- `POST /v1/embeddings` — Create embeddings
- `GET /v1/models` — List available models
- `GET /health` — Health check
- `GET /metrics` — Prometheus metrics
- `POST /v1/register`, `POST /v1/login`, `GET /v1/user`, `PUT /v1/user` — User management
