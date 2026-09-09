//go:build unit

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMonitorStreamDeltaExtractors(t *testing.T) {
	tests := []struct {
		name    string
		extract func([]byte) string
		data    string
		want    string
	}{
		{
			name:    "openai chat ignores role-only chunk",
			extract: extractOpenAIChatMonitorStreamDelta,
			data:    `{"choices":[{"delta":{"role":"assistant"}}]}`,
		},
		{
			name:    "openai chat reads text delta",
			extract: extractOpenAIChatMonitorStreamDelta,
			data:    `{"choices":[{"delta":{"content":"42"}}]}`,
			want:    "42",
		},
		{
			name:    "responses ignores reasoning delta",
			extract: extractOpenAIResponsesMonitorStreamDelta,
			data:    `{"type":"response.reasoning_summary_text.delta","delta":"hidden"}`,
		},
		{
			name:    "responses ignores function arguments",
			extract: extractOpenAIResponsesMonitorStreamDelta,
			data:    `{"type":"response.function_call_arguments.delta","delta":"{}"}`,
		},
		{
			name:    "responses reads output text delta",
			extract: extractOpenAIResponsesMonitorStreamDelta,
			data:    `{"type":"response.output_text.delta","delta":"42"}`,
			want:    "42",
		},
		{
			name:    "anthropic ignores thinking delta",
			extract: extractAnthropicMonitorStreamDelta,
			data:    `{"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"hidden"}}`,
		},
		{
			name:    "anthropic reads text delta",
			extract: extractAnthropicMonitorStreamDelta,
			data:    `{"type":"content_block_delta","delta":{"type":"text_delta","text":"42"}}`,
			want:    "42",
		},
		{
			name:    "gemini joins visible text parts and ignores thoughts",
			extract: extractGeminiMonitorStreamDelta,
			data:    `{"candidates":[{"content":{"parts":[{"text":"hidden","thought":true},{"text":"4"}]}},{"content":{"parts":[{"text":"2"}]}}]}`,
			want:    "42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.extract([]byte(tt.data)); got != tt.want {
				t.Fatalf("extract() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestForEachMonitorSSEDataHandlesCRLFMultilineAndDone(t *testing.T) {
	raw := []byte(": keepalive\r\ndata: {\"choices\":[\r\ndata: {\"delta\":{\"content\":\"42\"}}]}\r\n\r\ndata: [DONE]\r\n")
	var events []string
	if !forEachMonitorSSEData(raw, func(data []byte) {
		events = append(events, string(data))
	}) {
		t.Fatal("expected SSE data to be detected")
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if got := extractOpenAIChatMonitorStreamDelta([]byte(events[0])); got != "42" {
		t.Fatalf("multiline event text = %q, want 42", got)
	}
}

func TestRunCheckForModelRecordsFirstMeaningfulStreamingToken(t *testing.T) {
	swapMonitorHTTPClient(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { _ = r.Body.Close() }()
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		answer := answerFromOpenAIRequest(body)
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n")
		flusher.Flush()
		time.Sleep(10 * time.Millisecond)
		_, _ = fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", answer)
		flusher.Flush()
		time.Sleep(10 * time.Millisecond)
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(srv.Close)

	result := runCheckForModel(context.Background(), MonitorProviderOpenAI, srv.URL, "test-key", "gpt-test", nil)
	if result.Status != MonitorStatusOperational {
		t.Fatalf("status = %s, message = %q", result.Status, result.Message)
	}
	if result.FirstTokenMs == nil || *result.FirstTokenMs < 5 {
		t.Fatalf("first token = %v, want measured TTFT", result.FirstTokenMs)
	}
	if result.LatencyMs == nil || *result.LatencyMs < *result.FirstTokenMs {
		t.Fatalf("latency = %v, first token = %v", result.LatencyMs, result.FirstTokenMs)
	}
}

func TestRunCheckForModelDoesNotRecordTTFTForChallengeMismatch(t *testing.T) {
	swapMonitorHTTPClient(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"wrong answer\"}}]}\n\n")
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(srv.Close)

	result := runCheckForModel(context.Background(), MonitorProviderOpenAI, srv.URL, "test-key", "gpt-test", nil)
	if result.Status != MonitorStatusFailed {
		t.Fatalf("status = %s, want failed", result.Status)
	}
	if result.FirstTokenMs != nil {
		t.Fatalf("challenge mismatch must not record TTFT: %d", *result.FirstTokenMs)
	}
}

func TestRunCheckForModelDoesNotRecordTTFTForErrorSSE(t *testing.T) {
	swapMonitorHTTPClient(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"error text\"}}]}\n\n")
	}))
	t.Cleanup(srv.Close)

	result := runCheckForModel(context.Background(), MonitorProviderOpenAI, srv.URL, "test-key", "gpt-test", nil)
	if result.Status != MonitorStatusError {
		t.Fatalf("status = %s, want error", result.Status)
	}
	if result.FirstTokenMs != nil {
		t.Fatalf("error response must not record TTFT: %d", *result.FirstTokenMs)
	}
	if !strings.Contains(result.Message, "upstream HTTP 429") {
		t.Fatalf("unexpected error message: %q", result.Message)
	}
}

func TestRunCheckForModelStreamingAdaptersRecordTTFT(t *testing.T) {
	tests := []struct {
		name     string
		provider string
		options  *CheckOptions
	}{
		{name: "openai chat", provider: MonitorProviderOpenAI},
		{name: "grok chat", provider: MonitorProviderGrok},
		{name: "anthropic", provider: MonitorProviderAnthropic},
		{name: "gemini", provider: MonitorProviderGemini},
		{name: "openai responses", provider: MonitorProviderOpenAI, options: &CheckOptions{APIMode: MonitorAPIModeResponses}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			swapMonitorHTTPClient(t)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer func() { _ = r.Body.Close() }()
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				answer := answerFromOpenAIRequest(body)
				w.Header().Set("Content-Type", "text/event-stream")
				switch tt.provider {
				case MonitorProviderAnthropic:
					_, _ = fmt.Fprint(w, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"", answer, "\"}}\n\n")
				case MonitorProviderGemini:
					_, _ = fmt.Fprintf(w, "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":%q}]}}]}\n\n", answer)
				case MonitorProviderOpenAI:
					if tt.options != nil && tt.options.APIMode == MonitorAPIModeResponses {
						_, _ = fmt.Fprint(w, "data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"ignored\"}\n\n")
						_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":%q}\n\n", answer)
					} else {
						_, _ = fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", answer)
					}
				default:
					_, _ = fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", answer)
				}
				_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
			}))
			t.Cleanup(srv.Close)

			result := runCheckForModel(context.Background(), tt.provider, srv.URL, "test-key", "test-model", tt.options)
			if result.Status != MonitorStatusOperational {
				t.Fatalf("status = %s, message = %q", result.Status, result.Message)
			}
			if result.FirstTokenMs == nil {
				t.Fatal("streaming provider did not record TTFT")
			}
		})
	}
}

func TestRunCheckForModelReplaceStreamingRecordsTTFT(t *testing.T) {
	swapMonitorHTTPClient(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"custom response\"}}]}\n\n")
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(srv.Close)

	result := runCheckForModel(context.Background(), MonitorProviderOpenAI, srv.URL, "test-key", "test-model", &CheckOptions{
		BodyOverrideMode: MonitorBodyOverrideModeReplace,
		BodyOverride: map[string]any{
			"model":    "custom-model",
			"messages": []any{map[string]any{"role": "user", "content": "custom"}},
			"stream":   true,
		},
	})
	if result.Status != MonitorStatusOperational {
		t.Fatalf("status = %s, message = %q", result.Status, result.Message)
	}
	if result.FirstTokenMs == nil {
		t.Fatal("replace-mode streaming probe did not record TTFT")
	}
}

type captureMonitorHistoryRepo struct {
	ChannelMonitorRepository
	rows []*ChannelMonitorHistoryRow
}

func (r *captureMonitorHistoryRepo) InsertHistoryBatch(_ context.Context, rows []*ChannelMonitorHistoryRow) error {
	r.rows = rows
	return nil
}

func (r *captureMonitorHistoryRepo) MarkChecked(_ context.Context, _ int64, _ time.Time) error {
	return nil
}

func TestPersistCheckResultsCarriesNullableTTFTToHistory(t *testing.T) {
	token := 123
	repo := &captureMonitorHistoryRepo{}
	svc := &ChannelMonitorService{repo: repo}
	svc.persistCheckResults(context.Background(), &ChannelMonitor{ID: 9, Name: "test"}, []*CheckResult{
		{Model: "with-token", FirstTokenMs: &token},
		{Model: "without-token"},
	})

	if len(repo.rows) != 2 {
		t.Fatalf("persisted rows = %d, want 2", len(repo.rows))
	}
	if repo.rows[0].FirstTokenMs == nil || *repo.rows[0].FirstTokenMs != token {
		t.Fatalf("persisted TTFT = %v, want %d", repo.rows[0].FirstTokenMs, token)
	}
	if repo.rows[1].FirstTokenMs != nil {
		t.Fatalf("missing TTFT should remain nil, got %d", *repo.rows[1].FirstTokenMs)
	}
}

func TestReadMonitorStreamingBodyLeavesTTFTNilForInvalidEmptyAndOversizedStreams(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{name: "done only", body: "data: [DONE]\n\n"},
		{name: "malformed json", body: "data: {not-json}\n\n"},
		{name: "whitespace delta", body: `data: {"choices":[{"delta":{"content":"  "}}]}

`},
		{name: "oversized", body: strings.Repeat("x", monitorResponseMaxBytes+1), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, firstToken, err := readMonitorStreamingBody(strings.NewReader(tt.body), time.Now(), extractOpenAIChatMonitorStreamDelta)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %t", err, tt.wantErr)
			}
			if firstToken != nil {
				t.Fatalf("first token = %d, want nil", *firstToken)
			}
		})
	}
}
