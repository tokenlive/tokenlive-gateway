# 修复上游 User-Agent 限制 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 解决代理 `/messages` 及 `/v1/chat/completions` 等上游通道返回的关于限制 `Go-http-client/2.0` 的客户端报错，统一添加 User-Agent 透传与兜底伪装。

**Architecture:** 
1. 在向 Anthropic 代理发请求的 `anthropic_messages.go` 里，构建 `req` 时获取客户端真实 UA，若无则使用 Chrome 默认 UA 兜底。
2. 对 `openai.go` 的 OpenAI 兼容调用同理做 UA 代理设置。

**Tech Stack:** Go (Gateway Backend)

---

### Task 1: 修复 `anthropic_messages.go` 的 User-Agent 头

**Files:**
- Modify: `pkg/llm/providers/anthropic_messages.go`

- [ ] **Step 1: 注入 User-Agent 透传与兜底逻辑**

修改 `pkg/llm/providers/anthropic_messages.go` 在 `req.Header.Set("anthropic-version", ...)` 之后：
```go
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", ap.apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	ua := gctx.Request.Header.Get("User-Agent")
	if ua == "" {
		ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
	}
	req.Header.Set("User-Agent", ua)
```

---

### Task 2: 修复 `openai.go` 的 User-Agent 头

**Files:**
- Modify: `pkg/llm/providers/openai.go`

- [ ] **Step 1: 注入 User-Agent 透传与兜底逻辑**

修改 `pkg/llm/providers/openai.go` 的 `doRequest` 方法在 `req.Header.Set("Authorization", ...)` 之后：
```go
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.apiKey)

	ua := gctx.Request.Header.Get("User-Agent")
	if ua == "" {
		ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
	}
	req.Header.Set("User-Agent", ua)
```

---

### Task 3: 运行验证测试

- [ ] **Step 1: 执行编译校验**

运行：
```bash
make build
```
预期输出：编译成功。

- [ ] **Step 2: 运行现有单元测试验证无回归**

运行：
```bash
go test ./pkg/filters/outbound/ -v
```
预期输出：所有测试均 PASS。
