package core

import "testing"

func TestEndpointSupportsRequestType_ProtocolAwareFallback(t *testing.T) {
	tests := []struct {
		name     string
		protocol string
		declared []RequestType
		request  RequestType
		want     bool
	}{
		{
			name:     "anthropic endpoint can serve messages even when model declares chat",
			protocol: string(ProtocolAnthropic),
			declared: []RequestType{
				RequestTypeChatCompletion,
			},
			request: RequestTypeMessages,
			want:    true,
		},
		{
			name:     "anthropic endpoint cannot serve chat completion without a chat invoker",
			protocol: string(ProtocolAnthropic),
			declared: []RequestType{
				RequestTypeChatCompletion,
			},
			request: RequestTypeChatCompletion,
			want:    false,
		},
		{
			name:     "openai endpoint can translate messages when model declares chat",
			protocol: string(ProtocolOpenAI),
			declared: []RequestType{
				RequestTypeChatCompletion,
			},
			request: RequestTypeMessages,
			want:    true,
		},
		{
			name:     "joycode endpoint does not implicitly serve messages",
			protocol: string(ProtocolJoyCode),
			declared: []RequestType{
				RequestTypeChatCompletion,
			},
			request: RequestTypeMessages,
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ep := &Endpoint{
				ProviderProtocol: tt.protocol,
				RequestTypes:     tt.declared,
			}
			if got := ep.SupportsRequestType(tt.request); got != tt.want {
				t.Fatalf("SupportsRequestType(%s) = %v, want %v", tt.request, got, tt.want)
			}
		})
	}
}
