# 协议转换方案设计 (Protocol Translation) 实施方案

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现当客户端以 Anthropic `/v1/messages` 接口格式请求，但要访问的模型仅支持 OpenAI 协议时，在网关 Provider 适配层自动完成请求和响应（包含非流式与流式 SSE）的跨协议翻译与同名转发。

**Architecture:**

1. 扩展 OpenAI Provider 的 RequestTypes，加入 `RequestTypeMessages`。
2. 新建 `openaiMessagesInvoker`，在 `Invoke` 阶段将 Anthropic 协议体中的 `system`（系统提示词）和 `max_tokens` 翻译重构为 OpenAI Chat Completion 报文，其余非核心字段优雅透传。
3. 对非流式响应重写 JSON；对流式响应采用 `SSEParser` 解包上游 OpenAI 流帧并实时转换为 Anthropic 格式 SSE 流逐帧写回。

**Tech Stack:** Go 1.26+, Go-Redis, Gin, SSEParser

---

### Task 1: 编写非流式协议翻译核心逻辑及测试先行

**Files:**

- Create: `pkg/llm/providers/openai_messages_test.go`
- Create: `pkg/llm/providers/openai_messages.go`

- [ ] **Step 1: 编写非流式协议转换的失败测试**
  在 `openai_messages_test.go` 中，模拟 OpenAIProvider 接收 `RequestTypeMessages` 并校验非流式情况下的请求参数翻译与响应体翻译。
  测试代码结构如下：

  ```go
  package providers

  import (
   "context"
   "encoding/json"
   "net/http"
   "net/http/httptest"
   "strings"
   "testing"

   "tokenlive-gateway/pkg/core"
   "github.com/stretchr/testify/assert"
  )

  func TestOpenAIMessages_NonStream(t *testing.T) {
   // 模拟上游 OpenAI 服务
   server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    assert.Equal(t, "/chat/completions", r.URL.Path)
    var req map[string]interface{}
    _ = json.NewDecoder(r.Body).Decode(&req)
    
    // 验证参数翻译
    messages := req["messages"].([]interface{})
    assert.Equal(t, 2, len(messages)) // 包含 system + user
    assert.Equal(t, "system", messages[0].(map[string]interface{})["role"])
    assert.Equal(t, "You are a helpful assistant", messages[0].(map[string]interface{})["content"])
    
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusOK)
    w.Write([]byte(`{
     "id": "chatcmpl-123",
     "choices": [{"message": {"role": "assistant", "content": "hello world"}, "finish_reason": "stop"}],
     "usage": {"prompt_tokens": 10, "completion_tokens": 5}
    }`))
   }))
   defer server.Close()

   p := NewOpenAIProvider("test-openai", server.URL, "test-key", []string{"gpt-4"})
   
   // 模拟 Anthropic 请求 body
   reqBody := `{
    "model": "gpt-4",
    "system": "You are a helpful assistant",
    "messages": [{"role": "user", "content": "hi"}],
    "max_tokens": 100
   }`

   req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(reqBody))
   w := httptest.NewRecorder()
   gctx := core.AcquireContext(w, req)
   defer core.ReleaseContext(gctx)
   
   gctx.RequestType = core.RequestTypeMessages
   gctx.RawBody = []byte(reqBody)
   gctx.Model = "gpt-4"

   invoker := &openaiMessagesInvoker{}
   err := invoker.Invoke(gctx, p)
   assert.NoError(t, err)

   // 验证响应翻译
   var resp map[string]interface{}
   _ = json.Unmarshal(gctx.UpstreamBody, &resp)
   assert.Equal(t, "message", resp["type"])
   assert.Equal(t, "assistant", resp["role"])
   content := resp["content"].([]interface{})
   assert.Equal(t, "hello world", content[0].(map[string]interface{})["text"])
  }
  ```

- [ ] **Step 2: 运行测试以确保其编译或运行失败**
  在 `tokenlive-gateway` 目录运行测试命令，由于 `openaiMessagesInvoker` 未实现，测试应当编译失败或运行失败。
  运行：`go test -v ./pkg/llm/providers/ -run TestOpenAIMessages_NonStream`
  预期：编译失败，提示 `openaiMessagesInvoker` 未定义。

- [ ] **Step 3: 实现非流式请求参数与响应翻译**
  在 `pkg/llm/providers/openai_messages.go` 中实现 `openaiMessagesInvoker` 结构和非流式协议翻译逻辑：

  ```go
  package providers

  import (
   "bytes"
   "encoding/json"
   "fmt"
   "io"
   "net/http"

   "tokenlive-gateway/pkg/core"
  )

  type openaiMessagesInvoker struct{}

  func (i *openaiMessagesInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
   op, ok := p.(*OpenAIProvider)
   if !ok {
    return fmt.Errorf("expected *OpenAIProvider, got %T", p)
   }

   // 1. 翻译请求体 (Anthropic -> OpenAI)
   var payload map[string]interface{}
   if err := json.Unmarshal(gctx.RawBody, &payload); err != nil {
    return fmt.Errorf("parse raw body: %w", err)
   }

   // 处理 system prompt
   var openAIMessages []interface{}
   if systemPrompt, ok := payload["system"].(string); ok && systemPrompt != "" {
    openAIMessages = append(openAIMessages, map[string]string{
     "role":    "system",
     "content": systemPrompt,
    })
   }

   // 合并 messages
   if msgs, ok := payload["messages"].([]interface{}); ok {
    openAIMessages = append(openAIMessages, msgs...)
   }
   payload["messages"] = openAIMessages
   delete(payload, "system")

   // 映射 max_tokens
   if maxTokens, ok := payload["max_tokens"]; ok {
    payload["max_completion_tokens"] = maxTokens
    delete(payload, "max_tokens")
   }

   newBody, err := json.Marshal(payload)
   if err != nil {
    return fmt.Errorf("marshal translated body: %w", err)
   }
   gctx.RawBody = newBody

   // 2. 物理调用上游 completions 端点
   endpoint := op.baseURL + "/chat/completions"
   if err := op.doRequest(gctx, endpoint); err != nil {
    return err
   }

   // 3. 翻译响应体 (OpenAI -> Anthropic)
   if !gctx.IsStream {
    if err := translateNonStreamResponse(gctx); err != nil {
     return fmt.Errorf("translate response: %w", err)
    }
   }

   return nil
  }

  func translateNonStreamResponse(gctx *core.GatewayContext) error {
   var oaiResp struct {
    ID      string `json:"id"`
    Model   string `json:"model"`
    Choices []struct {
     Message struct {
      Role    string `json:"role"`
      Content string `json:"content"`
     } `json:"message"`
     FinishReason string `json:"finish_reason"`
    } `json:"choices"`
    Usage struct {
     PromptTokens     int `json:"prompt_tokens"`
     CompletionTokens int `json:"completion_tokens"`
    } `json:"usage"`
   }

   if err := json.Unmarshal(gctx.UpstreamBody, &oaiResp); err != nil {
    return err
   }

   stopReason := "end_turn"
   if len(oaiResp.Choices) > 0 && oaiResp.Choices[0].FinishReason == "length" {
    stopReason = "max_tokens"
   }

   content := ""
   if len(oaiResp.Choices) > 0 {
    content = oaiResp.Choices[0].Message.Content
   }

   anthropicResp := map[string]interface{}{
    "id":    oaiResp.ID,
    "type":  "message",
    "role":  "assistant",
    "model": gctx.Model,
    "content": []map[string]string{
     {
      "type": "text",
      "text": content,
     },
    },
    "stop_reason":    stopReason,
    "stop_sequence":  nil,
    "usage": map[string]int{
     "input_tokens":  oaiResp.Usage.PromptTokens,
     "output_tokens": oaiResp.Usage.CompletionTokens,
    },
   }

   translatedBody, err := json.Marshal(anthropicResp)
   if err != nil {
    return err
   }

   gctx.UpstreamBody = translatedBody
   return nil
  }
  ```

- [ ] **Step 4: 重新运行非流式测试以确保其通过**
  运行：`go test -v ./pkg/llm/providers/ -run TestOpenAIMessages_NonStream`
  预期：PASS

---

### Task 2: 实现流式响应的实时翻译

**Files:**

- Modify: `pkg/llm/providers/openai_messages_test.go`
- Modify: `pkg/llm/providers/openai_messages.go`
- Modify: `pkg/llm/providers/openai.go`

- [ ] **Step 1: 在测试中加入流式协议转换的失败测试**
  在 `openai_messages_test.go` 中，增加 `TestOpenAIMessages_Stream` 方法，验证上游 OpenAI 流被翻译转换为标准的 Anthropic SSE 事件帧：

  ```go
  func TestOpenAIMessages_Stream(t *testing.T) {
   server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "text/event-stream")
    w.WriteHeader(http.StatusOK)
    w.Write([]byte("data: {\"choices\": [{\"delta\": {\"content\": \"hello\"}}]}\n\n"))
    w.Write([]byte("data: {\"choices\": [{\"delta\": {\"content\": \" world\"}}]}\n\n"))
    w.Write([]byte("data: [DONE]\n\n"))
   }))
   defer server.Close()

   p := NewOpenAIProvider("test-openai", server.URL, "test-key", []string{"gpt-4"})
   
   reqBody := `{"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}], "stream": true}`
   req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(reqBody))
   w := httptest.NewRecorder()
   gctx := core.AcquireContext(w, req)
   defer core.ReleaseContext(gctx)
   
   gctx.RequestType = core.RequestTypeMessages
   gctx.RawBody = []byte(reqBody)
   gctx.Model = "gpt-4"
   gctx.IsStream = true

   invoker := &openaiMessagesInvoker{}
   err := invoker.Invoke(gctx, p)
   assert.NoError(t, err)

   // 验证写回的 SSE 流中包含 Anthropic 流格式
   respStr := w.Body.String()
   assert.Contains(t, respStr, "message_start")
   assert.Contains(t, respStr, "content_block_delta")
   assert.Contains(t, respStr, "hello")
   assert.Contains(t, respStr, "world")
   assert.Contains(t, respStr, "message_stop")
  }
  ```

- [ ] **Step 2: 运行流式测试以确保其失败**
  运行：`go test -v ./pkg/llm/providers/ -run TestOpenAIMessages_Stream`
  预期：FAIL，由于未处理 stream 分支（非流式翻译反序列化流响应会报错或失败）。

- [ ] **Step 3: 实现 handleMessagesStream 流翻译逻辑**
  在 `pkg/llm/providers/openai_messages.go` 中加入对流式响应的解码和实时重组写回：

  ```go
  // 在 Invoke 方法末尾加入分支：
  // if gctx.IsStream {
  //  return handleMessagesStream(gctx, gctx.UpstreamResponse)
  // }
  //
  // 实现如下：

  import (
   "tokenlive-gateway/pkg/llm"
  )

  func handleMessagesStream(gctx *core.GatewayContext, resp *http.Response) error {
   // 同样使用拦截器，但此处要由我们自己写回格式化好的 Anthropic 帧
   gctx.ResponseWriter.Header().Set("Content-Type", "text/event-stream")
   gctx.ResponseWriter.Header().Set("Cache-Control", "no-cache")
   gctx.ResponseWriter.Header().Set("Connection", "keep-alive")
   gctx.ResponseWriter.WriteHeader(http.StatusOK)

   // 1. 发送消息启动帧
   startFrame := `{"type": "message_start", "message": {"id": "msg-trans-stream", "type": "message", "role": "assistant", "content": []}}`
   _, _ = gctx.ResponseWriter.Write([]byte("data: " + startFrame + "\n\n"))
   
   blockStartFrame := `{"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}`
   _, _ = gctx.ResponseWriter.Write([]byte("data: " + blockStartFrame + "\n\n"))

   // 2. 实时读取上游事件并做转化
   parser := llm.NewSSEParser()
   buf := make([]byte, 4096)
   flusher, hasFlush := gctx.ResponseWriter.(http.Flusher)

   gctx.TriggerFirstByte()

   for {
    n, err := resp.Body.Read(buf)
    if n > 0 {
     events := parser.Feed(buf[:n])
     for _, ev := range events {
      if ev.Data == "[DONE]" {
       continue
      }

      // 解析 OpenAI 帧
      var chunk struct {
       Choices []struct {
        Delta struct {
         Content string `json:"content"`
        } `json:"delta"`
       } `json:"choices"`
      }
      if json.Unmarshal([]byte(ev.Data), &chunk) == nil && len(chunk.Choices) > 0 {
       txt := chunk.Choices[0].Delta.Content
       if txt != "" {
        gctx.TransmittedChars += len(txt)
        deltaFrame := fmt.Sprintf(`{"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": %q}}`, txt)
        _, _ = gctx.ResponseWriter.Write([]byte("data: " + deltaFrame + "\n\n"))
        if hasFlush {
         flusher.Flush()
        }
       }
      }
     }
    }
    if err != nil {
     if err == io.EOF {
      break
     }
     return fmt.Errorf("read upstream stream: %w", err)
    }
   }

   // 3. 发送消息结束帧
   blockStopFrame := `{"type": "content_block_stop", "index": 0}`
   _, _ = gctx.ResponseWriter.Write([]byte("data: " + blockStopFrame + "\n\n"))

   stopFrame := `{"type": "message_stop"}`
   _, _ = gctx.ResponseWriter.Write([]byte("data: " + stopFrame + "\n\n"))
   if hasFlush {
    flusher.Flush()
   }

   return nil
  }
  ```

- [ ] **Step 4: 运行测试以确保其通过**
  运行：`go test -v ./pkg/llm/providers/ -run TestOpenAIMessages_`
  预期：PASS（TestOpenAIMessages_NonStream 与 TestOpenAIMessages_Stream 全绿）

- [ ] **Step 5: 修改 Invoke 中的分支重定向**
  在 `pkg/llm/providers/openai_messages.go` 的 `Invoke` 末尾完善流分支重定向：

  ```go
   // 3. 翻译响应体 (OpenAI -> Anthropic)
   if gctx.IsStream {
    return handleMessagesStream(gctx, gctx.UpstreamResponse)
   } else {
    if err := translateNonStreamResponse(gctx); err != nil {
     return fmt.Errorf("translate response: %w", err)
    }
   }
  ```

---

### Task 3: 注册及能力装配集成

**Files:**

- Modify: `pkg/llm/providers/openai.go`
- Modify: `pkg/core/engine_test.go`

- [ ] **Step 1: 注册 openaiMessagesInvoker 并更新 RequestTypes**
  修改 `pkg/llm/providers/openai.go` 关联 `RequestTypeMessages` 能力：

  ```go
  // 在 init() 追加：
  core.RegisterRequestInvoker(core.ProviderOpenAI, core.RequestTypeMessages, &openaiMessagesInvoker{})

  // 在 RequestTypes() 数组追加：
  core.RequestTypeMessages,
  ```

- [ ] **Step 2: 编写集成测试校验全链路的协议翻译路由**
  在 `pkg/core/engine_test.go` 尾部追加 `TestEngine_HandleRequest_Messages_Translation`：

  ```go
  func TestEngine_HandleRequest_Messages_Translation(t *testing.T) {
   logger, _ := zap.NewDevelopment()
   ss := newMockStateStore()

   // 注册 OpenAI Provider 支持 Messages 的能力
   provider := &mockResponsesProvider{name: "openai"} // 复用之前的 mock provider

   ep := &Endpoint{
    ID:           "ep-trans-integration",
    Provider:     "openai",
    Model:        "gpt-4",
    RequestTypes: []RequestType{RequestTypeMessages}, // 声明支持 messages 
   }
   sd := NewStaticDiscovery()
   sd.RegisterService("gpt-4", []*Endpoint{ep})

   pipeline := &Pipeline{
    Name:         "messages",
    RequestTypes: []RequestType{RequestTypeMessages},
    Invoker: &mockResponsesInvoker{
     provider: provider,
     endpoint: ep,
    },
   }
   pipelines := map[string]*Pipeline{
    "messages": pipeline,
   }

   engine := newTestEngine(pipelines)
   engine.discovery = sd
   engine.stateStore = ss
   engine.logger = logger
   engine.providers = map[string]Provider{"openai": provider}

   reqBody := `{"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}], "max_tokens": 50}`
   req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(reqBody))
   req.Header.Set("Content-Type", "application/json")

   rec := httptest.NewRecorder()
   engine.HandleRequest(rec, req)

   assert.Equal(t, http.StatusOK, rec.Code)
  }
  ```

- [ ] **Step 3: 运行网关全包测试验证无误**
  运行：`go test ./pkg/core/... ./pkg/filters/... ./pkg/llm/providers/...`
  预期：所有单元测试及集成测试通过。

- [ ] **Step 4: 本地编译打包**
  运行：`make build`
  预期：网关编译成功，二进制产物无误生成。
