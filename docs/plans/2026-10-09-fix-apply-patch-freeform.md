# Support Freeform Tool (apply_patch) in Responses Protocol Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable transparent bidirectional conversion for freeform custom tools (specifically `apply_patch`) between OpenAI Responses API and upstream Chat Completion models, fixing the "Fatal error: tool apply_patch invoked with incompatible payload" error in Codex.

**Architecture:** 
1. In `pkg/llm/translate/helpers.go`, implement `EnsureCustomToolCallItemID` and `ExtractPatchInput` to normalize custom tool IDs and extract diff text from JSON arguments.
2. In `pkg/llm/translate/tool_name_mapper.go`, track tools that originated as `custom` or `apply_patch`.
3. In `pkg/llm/translate/responses_chat.go`, support `custom_tool_call` and `custom_tool_call_output` in conversation history, and convert non-stream Chat tool calls to `custom_tool_call` with `input`.
4. In `pkg/llm/providers/openai_responses.go`, in `handleResponsesStream`, detect `apply_patch`/custom tool calls, suppress function argument deltas, unwrap the patch string, and emit standard `custom_tool_call` output item added/done events with the `input` field.

**Tech Stack:** Go 1.25+, Gin, Zap, Testify.

**Spec:** Bidirectional compatibility between OpenAI Responses API (`custom_tool_call` / freeform) and Chat Completion Function Calling for tools like `apply_patch`.

## Global Constraints
- Preserve full backward compatibility for existing `function_call` tools (e.g. `exec_command`, `read_file`).
- Zero extra external dependencies.
- Pass all existing tests in `pkg/llm/translate/` and `pkg/llm/providers/`.

---

### Task 1: Add Custom Tool Helpers and Name Mapper Custom Detection

**Files:**
- Modify: `pkg/llm/translate/helpers.go`
- Modify: `pkg/llm/translate/tool_name_mapper.go`
- Test: `pkg/llm/translate/helpers_test.go`
- Test: `pkg/llm/translate/tool_name_mapper_test.go`

**Interfaces:**
- Produces:
  - `EnsureCustomToolCallItemID(id string) string`
  - `ExtractPatchInput(args string) string`
  - `(m *ToolNameMapper) RegisterCustom(name string)`
  - `(m *ToolNameMapper) IsCustom(name string) bool`

- [ ] **Step 1: Write unit tests for `EnsureCustomToolCallItemID` and `ExtractPatchInput`**

Add tests to `pkg/llm/translate/helpers_test.go`:
```go
func TestEnsureCustomToolCallItemID(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", "ctc_mock"},
		{"ctc_123", "ctc_123"},
		{"call_123", "ctc_123"},
		{"fc_123", "ctc_123"},
		{"plain123", "ctc_plain123"},
	}
	for _, tc := range tests {
		actual := EnsureCustomToolCallItemID(tc.input)
		if actual != tc.expected {
			t.Errorf("EnsureCustomToolCallItemID(%q) = %q, want %q", tc.input, actual, tc.expected)
		}
	}
}

func TestExtractPatchInput(t *testing.T) {
	tests := []struct {
		args     string
		expected string
	}{
		{`{"patch":"*** Begin Patch\n+line\n*** End Patch"}`, "*** Begin Patch\n+line\n*** End Patch"},
		{`{"diff":"diff --git a b"}`, "diff --git a b"},
		{`{"content":"some text"}`, "some text"},
		{`{"input":"raw input"}`, "raw input"},
		{`*** Begin Patch\nraw`, `*** Begin Patch\nraw`},
		{`plain string`, "plain string"},
	}
	for _, tc := range tests {
		actual := ExtractPatchInput(tc.args)
		if actual != tc.expected {
			t.Errorf("ExtractPatchInput(%q) = %q, want %q", tc.args, actual, tc.expected)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./pkg/llm/translate -run "TestEnsureCustomToolCallItemID|TestExtractPatchInput"`
Expected: FAIL (functions not defined)

- [ ] **Step 3: Implement helpers and ToolNameMapper custom tracking**

In `pkg/llm/translate/helpers.go`:
```go
// EnsureCustomToolCallItemID normalizes a tool call ID to the OpenAI Responses ctc_ prefix for custom tool call item IDs.
func EnsureCustomToolCallItemID(id string) string {
	if id == "" {
		return "ctc_mock"
	}
	if strings.HasPrefix(id, "ctc_") {
		return id
	}
	res := strings.TrimPrefix(id, "call_")
	res = strings.TrimPrefix(res, "fc_")
	res = strings.TrimPrefix(res, "toolu_")
	res = strings.TrimPrefix(res, "toolu-")
	if res == "" {
		return "ctc_mock"
	}
	return "ctc_" + res
}

// ExtractPatchInput extracts raw patch text from arguments.
func ExtractPatchInput(args string) string {
	trimmed := strings.TrimSpace(args)
	if strings.HasPrefix(trimmed, "{") {
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(trimmed), &m); err == nil {
			for _, key := range []string{"patch", "diff", "content", "input"} {
				if val, ok := m[key].(string); ok {
					return val
				}
			}
		}
	}
	return args
}
```

In `pkg/llm/translate/tool_name_mapper.go`, add `customTools map[string]bool`:
```go
func (m *ToolNameMapper) RegisterCustom(name string) {
	if m.customTools == nil {
		m.customTools = make(map[string]bool)
	}
	m.customTools[name] = true
}

func (m *ToolNameMapper) IsCustom(name string) bool {
	if name == "apply_patch" {
		return true
	}
	if m == nil || m.customTools == nil {
		return false
	}
	return m.customTools[name]
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./pkg/llm/translate -run "TestEnsureCustomToolCallItemID|TestExtractPatchInput"`
Expected: PASS

---

### Task 2: Support `custom_tool_call` in Conversation History and Non-Stream Translation

**Files:**
- Modify: `pkg/llm/translate/responses_chat.go`
- Test: `pkg/llm/translate/responses_chat_test.go`

**Interfaces:**
- Consumes: `ExtractPatchInput`, `EnsureCustomToolCallItemID`, `ToolNameMapper`
- Produces: Correct `openAIMessages` when input has `custom_tool_call`/`custom_tool_call_output`, and `custom_tool_call` in `ChatCompletionToResponses`.

- [ ] **Step 1: Write tests for `custom_tool_call` history and non-stream translation**

Add tests to `pkg/llm/translate/responses_chat_test.go`:
- `TestResponsesRequestToChat_CustomToolCallHistory`: Verify that history containing `custom_tool_call` and `custom_tool_call_output` is converted to upstream assistant tool_calls with `{"patch": "..."}` arguments and tool message with output.
- `TestChatCompletionToResponses_ApplyPatchCustomToolCall`: Verify that ChatCompletion response containing tool call `apply_patch` with arguments `{"patch": "diff text"}` is translated to `type: "custom_tool_call"`, `name: "apply_patch"`, `input: "diff text"`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -v ./pkg/llm/translate -run "TestResponsesRequestToChat_CustomToolCallHistory|TestChatCompletionToResponses_ApplyPatchCustomToolCall"`
Expected: FAIL

- [ ] **Step 3: Update `responses_chat.go`**

1. In first pass (indexing toolOutputs):
```go
if itemType == "function_call_output" || itemType == "custom_tool_call_output" {
    if callID := responseItemCallID(itemMap); callID != "" {
        toolOutputs[callID] = functionCallOutputText(itemMap)
    }
}
```
2. In second pass (collecting pending tool calls):
```go
if itemType == "custom_tool_call" {
    callID := responseItemCallID(itemMap)
    if _, ok := toolOutputs[callID]; !ok {
        continue
    }
    name, _ := itemMap["name"].(string)
    inputStr, _ := itemMap["input"].(string)
    argsJSON, _ := json.Marshal(map[string]string{"patch": inputStr})
    pendingToolCalls = append(pendingToolCalls, map[string]interface{}{
        "id": callID,
        "type": "function",
        "function": map[string]interface{}{
            "name": name,
            "arguments": string(argsJSON),
        },
    })
    continue
}
if itemType == "custom_tool_call_output" {
    callID := responseItemCallID(itemMap)
    if callID == "" || pendingAnswered[callID] || !pendingHasCallID(pendingToolCalls, callID) {
        flushPendingToolCalls()
    }
    continue
}
```
3. In `ChatCompletionToResponses`:
```go
isCustom := toolName == "apply_patch" || (mapper != nil && mapper.IsCustom(toolName))
if isCustom {
    outputList = append(outputList, map[string]interface{}{
        "id": EnsureCustomToolCallItemID(tc.ID),
        "call_id": tc.ID,
        "type": "custom_tool_call",
        "status": "completed",
        "name": toolName,
        "input": ExtractPatchInput(tc.Function.Arguments),
    })
} else {
    // existing function_call code
}
```
4. In `flattenResponsesTool`:
```go
if innerName == "apply_patch" || toolType == "custom" {
    mapper.RegisterCustom(innerName)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -v ./pkg/llm/translate -run "TestResponsesRequestToChat_CustomToolCallHistory|TestChatCompletionToResponses_ApplyPatchCustomToolCall"`
Expected: PASS

---

### Task 3: Support `custom_tool_call` in Responses Stream (`handleResponsesStream`)

**Files:**
- Modify: `pkg/llm/providers/openai_responses.go`
- Test: `pkg/llm/providers/openai_responses_test.go`

**Interfaces:**
- Consumes: `ExtractPatchInput`, `EnsureCustomToolCallItemID`, `ToolNameMapper`
- Produces: Correct SSE stream with `custom_tool_call` on `apply_patch`.

- [ ] **Step 1: Write test for streaming `apply_patch` translation**

Add `TestHandleResponsesStream_ApplyPatchCustomToolCall` in `pkg/llm/providers/openai_responses_test.go`:
Stream Chat Completion chunks containing tool call `apply_patch` with arguments delta `{"patch": "*** Begin Patch\n+test\n*** End Patch"}`.
Verify:
1. `response.output_item.added` has `item.type == "custom_tool_call"` and `item.input == ""`.
2. No `response.function_call.arguments.delta` is emitted for `apply_patch`.
3. `response.output_item.done` has `item.type == "custom_tool_call"` and `item.input == "*** Begin Patch\n+test\n*** End Patch"`.
4. `response.completed` has `output` item with `type == "custom_tool_call"` and `input == "*** Begin Patch\n+test\n*** End Patch"`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./pkg/llm/providers -run TestHandleResponsesStream_ApplyPatchCustomToolCall`
Expected: FAIL

- [ ] **Step 3: Update `handleResponsesStream` in `pkg/llm/providers/openai_responses.go`**

1. In `localToolCall` struct:
Add `IsCustom bool`.
2. When creating `localTC`:
```go
isCustom := localTC.Name == "apply_patch" || (toolMapper != nil && toolMapper.IsCustom(localTC.Name))
localTC.IsCustom = isCustom
if isCustom {
    localTC.ID = translate.EnsureCustomToolCallItemID(localTC.CallID)
} else {
    localTC.ID = translate.EnsureFunctionCallItemID(localTC.CallID)
}
```
3. When emitting `response.output_item.added`:
If `localTC.IsCustom`:
Emit custom tool call item with `Type: "custom_tool_call"` and `Input: ""`.
Else:
Emit function call item with `Type: "function_call"` and `Arguments: ""`.
4. In delta loop:
If `!localTC.IsCustom`:
Emit `response.function_call.arguments.delta`.
(For custom tool calls, delta arguments are silently accumulated in `localTC.Arguments`).
5. When finalizing tool calls (`response.function_call.arguments.done` and `response.output_item.done`):
If `tc.IsCustom`:
Extract raw patch text using `translate.ExtractPatchInput(finalArgs)`.
Do NOT emit `response.function_call.arguments.done`.
Emit `response.output_item.done` with `type: "custom_tool_call"`, `name: tc.Name`, `input: extractedPatch`.
In `response.completed` output list, append `map[string]interface{}{"id": tc.ID, "call_id": tc.CallID, "type": "custom_tool_call", "status": "completed", "name": tc.Name, "input": extractedPatch}`.
Else:
Emit existing function call events.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./pkg/llm/providers -run TestHandleResponsesStream_ApplyPatchCustomToolCall`
Expected: PASS

---

### Task 4: Full Test Suite Validation

**Files:**
- Test: all packages in `pkg/llm/...`

- [ ] **Step 1: Run all translation and provider tests**
Run: `go test -v ./pkg/llm/translate/... ./pkg/llm/providers/...`
Expected: ALL PASS

- [ ] **Step 2: Build binary to ensure clean compilation**
Run: `go build ./...`
Expected: Clean exit 0
