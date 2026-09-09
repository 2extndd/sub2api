//go:build unit

package service

import (
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/domain"
)

func mustParseConversationAffinityRequest(t *testing.T, protocol, body string) *ParsedRequest {
	t.Helper()
	parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(body)), protocol)
	if err != nil {
		t.Fatalf("ParseGatewayRequest() error = %v", err)
	}
	parsed.SessionContext = &SessionContext{
		ClientIP:  "198.51.100.10",
		UserAgent: "codex-cli/1.2.3",
		APIKeyID:  42,
	}
	return parsed
}

func TestGenerateSessionHash_ChatGrowingConversationKeepsStableAffinity(t *testing.T) {
	svc := &GatewayService{}
	first := mustParseConversationAffinityRequest(t, "chat_completions", `{
		"model":"gpt-5.6",
		"messages":[
			{"role":"system","content":"You are concise."},
			{"role":"user","content":"Explain cache locality."}
		]
	}`)
	grown := mustParseConversationAffinityRequest(t, "chat_completions", `{
		"model":"gpt-5.6",
		"messages":[
			{"role":"system","content":"You are concise."},
			{"role":"user","content":"Explain cache locality."},
			{"role":"assistant","content":"Cache locality keeps related work together."},
			{"role":"user","content":"Give an example."}
		]
	}`)

	firstHash := svc.GenerateSessionHash(first)
	grownHash := svc.GenerateSessionHash(grown)
	if firstHash == "" {
		t.Fatal("GenerateSessionHash() returned an empty hash")
	}
	if firstHash != grownHash {
		t.Fatalf("growing Chat conversation changed affinity: first=%q grown=%q", firstHash, grownHash)
	}
}

func TestGenerateSessionHash_MessagesGrowingConversationKeepsStableAffinity(t *testing.T) {
	svc := &GatewayService{}
	first := mustParseConversationAffinityRequest(t, domain.PlatformAnthropic, `{
		"model":"claude-sonnet-4-6",
		"system":"You are concise.",
		"messages":[
			{"role":"user","content":"Explain cache locality."}
		]
	}`)
	grown := mustParseConversationAffinityRequest(t, domain.PlatformAnthropic, `{
		"model":"claude-sonnet-4-6",
		"system":"You are concise.",
		"messages":[
			{"role":"user","content":"Explain cache locality."},
			{"role":"assistant","content":"Cache locality keeps related work together."},
			{"role":"user","content":"Give an example."}
		]
	}`)

	if got, want := svc.GenerateSessionHash(grown), svc.GenerateSessionHash(first); got != want {
		t.Fatalf("growing Messages conversation changed affinity: got=%q want=%q", got, want)
	}
}

func TestGenerateSessionHash_ChatNonTextFirstTurnKeepsStableDistinctAffinity(t *testing.T) {
	svc := &GatewayService{}
	first := mustParseConversationAffinityRequest(t, "chat_completions", `{
		"model":"gpt-5.6",
		"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}]}]
	}`)
	grown := mustParseConversationAffinityRequest(t, "chat_completions", `{
		"model":"gpt-5.6",
		"messages":[
			{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}]},
			{"role":"assistant","content":"I can see the image."},
			{"role":"user","content":"Describe it."}
		]
	}`)
	different := mustParseConversationAffinityRequest(t, "chat_completions", `{
		"model":"gpt-5.6",
		"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,BBBB"}}]}]
	}`)

	firstHash := svc.GenerateSessionHash(first)
	if got := svc.GenerateSessionHash(grown); got != firstHash {
		t.Fatalf("growing non-text conversation changed affinity: got=%q want=%q", got, firstHash)
	}
	if got := svc.GenerateSessionHash(different); got == firstHash {
		t.Fatal("different non-text first turns produced the same affinity hash")
	}
}

func TestGenerateSessionHash_ChatToolFirstTurnKeepsStableAffinity(t *testing.T) {
	svc := &GatewayService{}
	first := mustParseConversationAffinityRequest(t, "chat_completions", `{
		"model":"gpt-5.6",
		"messages":[{"role":"tool","tool_call_id":"call-1","content":"result"}]
	}`)
	grown := mustParseConversationAffinityRequest(t, "chat_completions", `{
		"model":"gpt-5.6",
		"messages":[
			{"role":"tool","tool_call_id":"call-1","content":"result"},
			{"role":"user","content":"Continue."}
		]
	}`)

	if got, want := svc.GenerateSessionHash(grown), svc.GenerateSessionHash(first); got != want {
		t.Fatalf("growing tool-first conversation changed affinity: got=%q want=%q", got, want)
	}
}

func TestGenerateSessionHash_ChatAffinitySeparatesConversationAndModel(t *testing.T) {
	svc := &GatewayService{}
	base := mustParseConversationAffinityRequest(t, "chat_completions", `{
		"model":"gpt-5.6",
		"messages":[{"role":"user","content":"First conversation"}]
	}`)
	differentConversation := mustParseConversationAffinityRequest(t, "chat_completions", `{
		"model":"gpt-5.6",
		"messages":[{"role":"user","content":"Second conversation"}]
	}`)
	differentModel := mustParseConversationAffinityRequest(t, "chat_completions", `{
		"model":"gpt-5.6-codex",
		"messages":[{"role":"user","content":"First conversation"}]
	}`)

	baseHash := svc.GenerateSessionHash(base)
	if baseHash == svc.GenerateSessionHash(differentConversation) {
		t.Fatal("different first user messages produced the same affinity hash")
	}
	if baseHash == svc.GenerateSessionHash(differentModel) {
		t.Fatal("different requested models produced the same affinity hash")
	}
}

func TestGenerateSessionHash_ChatReusablePrefixGroupsIndependentPrompts(t *testing.T) {
	svc := &GatewayService{}
	first := mustParseConversationAffinityRequest(t, "chat_completions", `{
		"model":"gpt-5.6",
		"messages":[
			{"role":"system","content":"Stable policy"},
			{"role":"user","content":"First task"}
		]
	}`)
	second := mustParseConversationAffinityRequest(t, "chat_completions", `{
		"model":"gpt-5.6",
		"messages":[
			{"role":"system","content":"Stable policy"},
			{"role":"user","content":"Second task"}
		]
	}`)

	if got, want := svc.GenerateSessionHash(second), svc.GenerateSessionHash(first); got != want {
		t.Fatalf("same reusable Chat prefix should share affinity: got=%q want=%q", got, want)
	}
}

func TestGenerateSessionHash_AnthropicReusablePrefixGroupsIndependentPrompts(t *testing.T) {
	svc := &GatewayService{}
	first := mustParseConversationAffinityRequest(t, domain.PlatformAnthropic, `{
		"model":"claude-sonnet-4-6",
		"system":"Stable Claude policy",
		"messages":[{"role":"user","content":"First task"}]
	}`)
	second := mustParseConversationAffinityRequest(t, domain.PlatformAnthropic, `{
		"model":"claude-sonnet-4-6",
		"system":"Stable Claude policy",
		"messages":[{"role":"user","content":"Second task"}]
	}`)

	if got, want := svc.GenerateSessionHash(second), svc.GenerateSessionHash(first); got != want {
		t.Fatalf("same reusable Messages prefix should share affinity: got=%q want=%q", got, want)
	}
}

func TestGenerateSessionHash_GeminiReusablePrefixAndGrowingConversationStayStable(t *testing.T) {
	svc := &GatewayService{}
	first := mustParseConversationAffinityRequest(t, domain.PlatformGemini, `{
		"model":"gemini-2.5-pro",
		"systemInstruction":{"parts":[{"text":"Stable Gemini policy"}]},
		"contents":[{"role":"user","parts":[{"text":"First task"}]}]
	}`)
	independent := mustParseConversationAffinityRequest(t, domain.PlatformGemini, `{
		"model":"gemini-2.5-pro",
		"systemInstruction":{"parts":[{"text":"Stable Gemini policy"}]},
		"contents":[{"role":"user","parts":[{"text":"Second task"}]}]
	}`)
	grown := mustParseConversationAffinityRequest(t, domain.PlatformGemini, `{
		"model":"gemini-2.5-pro",
		"systemInstruction":{"parts":[{"text":"Stable Gemini policy"}]},
		"contents":[
			{"role":"user","parts":[{"text":"First task"}]},
			{"role":"model","parts":[{"text":"First response"}]},
			{"role":"user","parts":[{"text":"Follow-up"}]}
		]
	}`)

	firstHash := svc.GenerateSessionHash(first)
	if got := svc.GenerateSessionHash(independent); got != firstHash {
		t.Fatalf("same reusable Gemini prefix should share affinity: got=%q want=%q", got, firstHash)
	}
	if got := svc.GenerateSessionHash(grown); got != firstHash {
		t.Fatalf("growing Gemini conversation changed affinity: got=%q want=%q", got, firstHash)
	}
}

func TestGenerateSessionHash_ResponsesReusablePrefixGroupsIndependentPrompts(t *testing.T) {
	svc := &GatewayService{}
	first := mustParseConversationAffinityRequest(t, "responses", `{
		"model":"gpt-5.6",
		"instructions":"Stable Responses policy",
		"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}],
		"input":[{"role":"user","content":"First task"}]
	}`)
	second := mustParseConversationAffinityRequest(t, "responses", `{
		"model":"gpt-5.6",
		"instructions":"Stable Responses policy",
		"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}],
		"input":[{"role":"user","content":"Second task"}]
	}`)

	if got, want := svc.GenerateSessionHash(second), svc.GenerateSessionHash(first); got != want {
		t.Fatalf("same reusable Responses prefix should share affinity: got=%q want=%q", got, want)
	}
}
