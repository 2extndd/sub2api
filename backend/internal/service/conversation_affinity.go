package service

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"unsafe"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/tidwall/gjson"
)

const (
	chatCompletionsProtocol = "chat_completions"
	responsesProtocol       = "responses"
)

// usesStableConversationAffinity identifies stateless conversation protocols whose
// messages array grows on every turn. Their sticky key must use a stable prefix,
// not the complete request history, or each turn can be routed to a new account.
func usesStableConversationAffinity(protocol string) bool {
	return protocol == chatCompletionsProtocol ||
		protocol == responsesProtocol ||
		protocol == domain.PlatformAnthropic ||
		protocol == domain.PlatformGemini
}

// appendStableConversationAffinity appends a versioned, length-delimited routing
// anchor. The resulting builder is hashed by GenerateSessionHash; no prompt text
// is persisted or logged. System/developer instructions and the first user turn
// remain stable as later turns are appended.
func appendStableConversationAffinity(builder *strings.Builder, parsed *ParsedRequest) bool {
	if builder == nil || parsed == nil {
		return false
	}

	if parsed.Body != nil {
		if stablePrefix := deriveOpenAIStablePrefixSessionSeed(parsed.Body.Bytes()); stablePrefix != "" {
			openAIPromptCacheStablePrefixTotal.Add(1)
			appendConversationAffinityField(builder, "affinity", "prompt-prefix-v1")
			appendConversationAffinityField(builder, "protocol", parsed.protocol)
			appendConversationAffinityField(builder, "model", parsed.Model)
			appendConversationAffinityField(builder, "stable_prefix", stablePrefix)
			return true
		}
	}

	var anchor strings.Builder
	if !appendStableMessagePrefix(&anchor, parsed.MessagesRaw()) {
		return false
	}

	openAIPromptCacheAnchoredFallbackTotal.Add(1)
	appendConversationAffinityField(builder, "affinity", "conversation-v1")
	appendConversationAffinityField(builder, "protocol", parsed.protocol)
	appendConversationAffinityField(builder, "model", parsed.Model)
	builder.WriteString(anchor.String())
	return true
}

func appendStableMessagePrefix(builder *strings.Builder, raw []byte) bool {
	if builder == nil || len(raw) == 0 {
		return false
	}

	messages := parseRawJSONView(raw)
	if !messages.IsArray() {
		return false
	}

	appended := false
	messages.ForEach(func(_, message gjson.Result) bool {
		role := strings.ToLower(strings.TrimSpace(message.Get("role").String()))
		content := message.Get("content")
		text := extractTextFromContentRaw(content)

		if role == "system" || role == "developer" {
			if text != "" {
				appendConversationAffinityField(builder, role, text)
				appended = true
			} else if digest := conversationAffinityDigest(message.Raw); digest != "" {
				appendConversationAffinityField(builder, role+"_digest", digest)
				appended = true
			}
			return true
		}

		if text != "" && (role == "user" || role == "assistant") {
			appendConversationAffinityField(builder, role, text)
			appended = true
		} else if digest := conversationAffinityDigest(message.Raw); digest != "" {
			// Hash the complete first non-system message for multimodal and tool
			// turns. Raw content never leaves process memory or enters logs.
			appendConversationAffinityField(builder, role+"_digest", digest)
			appended = true
		} else {
			return true
		}
		return false
	})
	return appended
}

func conversationAffinityDigest(raw string) string {
	if raw == "" {
		return ""
	}
	rawBytes := unsafe.Slice(unsafe.StringData(raw), len(raw))
	sum := sha256.Sum256(rawBytes)
	return hex.EncodeToString(sum[:])
}

func appendConversationAffinityField(builder *strings.Builder, name, value string) {
	builder.WriteByte(0x1f)
	builder.WriteString(name)
	builder.WriteByte('=')
	builder.WriteString(strconv.Itoa(len(value)))
	builder.WriteByte(':')
	builder.WriteString(value)
}
