package providers

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

func TestChatDialectProviderRequests(t *testing.T) {
	const developer = "chat_developer_to_system"
	const store = "chat_store_unsupported"
	active := []string{developer, store}
	for _, provider := range []string{"openai", "joycode"} {
		t.Run(provider, func(t *testing.T) {
			chatPath, responsesPath := "/chat/completions", "/responses"
			if provider == "joycode" {
				chatPath, responsesPath = "/api/saas/openai/v2/chat/completions", "/api/saas/openai/v2/responses/completions"
			}
			for _, tc := range []struct {
				name, body, path, errorMessage string
				requestType                    core.RequestType
				declared                       []core.RequestType
				dialects                       []string
				expect                         func(*testing.T, string)
			}{
				{
					name: "no rules", body: `{"model":"m","messages":[{"role":"developer"}],"store":true}`,
					path: chatPath, requestType: core.RequestTypeChatCompletion,
					expect: func(t *testing.T, body string) {
						require.Contains(t, body, `"role":"developer"`)
						require.Contains(t, body, `"store":true`)
					},
				},
				{
					name: "both rules", body: `{"model":"m","messages":[{"role":"developer","content":"instruction"},{"role":"user","content":"hi"}],"store":false,"vendor":{"role":"developer","store":true}}`,
					path: chatPath, requestType: core.RequestTypeChatCompletion, dialects: active,
					expect: func(t *testing.T, body string) {
						require.Contains(t, body, `"content":"instruction","role":"system"`)
						require.Contains(t, body, `"role":"user"`)
						require.NotContains(t, body, `"store":false`)
						var sent struct {
							Vendor json.RawMessage `json:"vendor"`
						}
						require.NoError(t, json.Unmarshal([]byte(body), &sent))
						require.JSONEq(t, `{"role":"developer","store":true}`, string(sent.Vendor))
					},
				},
				{
					name: "other store type", body: `{"model":"m","messages":[],"store":"false"}`,
					path: chatPath, requestType: core.RequestTypeChatCompletion, dialects: active,
					expect: func(t *testing.T, body string) { require.Contains(t, body, `"store":"false"`) },
				},
				{
					name: "store rejected", body: `{"model":"m","messages":[],"store":true}`,
					requestType: core.RequestTypeChatCompletion, dialects: active, errorMessage: "store=true",
				},
				{
					name: "conflicting roles rejected", body: `{"model":"m","messages":[{"role":"developer"},{"role":"system"}]}`,
					requestType: core.RequestTypeChatCompletion, dialects: active, errorMessage: "system and developer",
				},
				{
					name: "native responses", body: `{"model":"m","input":"hi","store":true}`,
					path: responsesPath, requestType: core.RequestTypeResponses, dialects: active,
					declared: []core.RequestType{core.RequestTypeResponses, core.RequestTypeChatCompletion},
					expect:   func(t *testing.T, body string) { require.Contains(t, body, `"store":true`) },
				},
				{
					name: "translated responses", body: `{"model":"m","instructions":"instruction","input":"hi","store":true}`,
					path: chatPath, requestType: core.RequestTypeResponses, dialects: active,
					declared: []core.RequestType{core.RequestTypeChatCompletion},
					expect: func(t *testing.T, body string) {
						require.NotContains(t, body, `"store"`)
						require.Contains(t, body, `"role":"system"`)
					},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					type request struct{ path, body string }
					received := make(chan request, 1)
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						body, _ := io.ReadAll(r.Body)
						received <- request{r.URL.Path, string(body)}
						w.Header().Set("Content-Type", "application/json")
						if r.URL.Path == responsesPath {
							_, _ = io.WriteString(w, `{"id":"resp_1","object":"response","output":[]}`)
						} else {
							_, _ = io.WriteString(w, `{"id":"chatcmpl-1","object":"chat.completion","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
						}
					}))
					defer server.Close()
					var p core.Provider = NewOpenAIProvider("test", server.URL, "test-key", nil)
					if provider == "joycode" {
						p = NewJoyCodeProvider("test", server.URL, "test-key", nil)
					}
					gctx := core.AcquireContext(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.body)))
					defer core.ReleaseContext(gctx)
					gctx.RawBody = []byte(tc.body)
					gctx.Model, gctx.RequestType = "m", tc.requestType
					gctx.SelectedEndpoint = &core.Endpoint{RequestTypes: tc.declared, DialectImplementations: tc.dialects}
					err := p.Invoke(gctx)
					require.Equal(t, tc.body, string(gctx.RawBody))
					if tc.errorMessage != "" {
						var rejection core.ChatDialectError
						require.ErrorAs(t, err, &rejection)
						require.ErrorContains(t, err, tc.errorMessage)
						require.Empty(t, received)
						return
					}
					require.NoError(t, err)
					require.Len(t, received, 1)
					got := <-received
					require.Equal(t, tc.path, got.path)
					tc.expect(t, got.body)
				})
			}
		})
	}
}

func TestChatDialectsLeaveGeminiRequestUntouched(t *testing.T) {
	body := `{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"store":true}`
	received := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		received <- string(data)
		_, _ = io.WriteString(w, `{"candidates":[{"content":{"parts":[{"text":"ok"}]}}]}`)
	}))
	defer server.Close()

	p := NewGeminiProvider("test", server.URL, "test-key", nil)
	gctx := core.AcquireContext(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)))
	defer core.ReleaseContext(gctx)
	gctx.RawBody = []byte(body)
	gctx.RequestType, gctx.Model = core.RequestTypeGeminiGenerateContent, "gemini"
	gctx.SelectedEndpoint = &core.Endpoint{DialectImplementations: []string{"chat_store_unsupported"}}
	require.NoError(t, p.Invoke(gctx))
	require.Len(t, received, 1)
	require.Equal(t, body, <-received)
}
