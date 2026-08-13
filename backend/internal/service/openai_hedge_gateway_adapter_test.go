package service

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type openAIHedgeGatewayEmitter struct {
	events []OpenAIHedgeEvent
}

func (emitter *openAIHedgeGatewayEmitter) EmitOpenAIHedgeEvent(event OpenAIHedgeEvent) error {
	emitter.events = append(emitter.events, cloneOpenAIHedgeEvent(event))
	return nil
}

func TestClassifyOpenAIHedgeSSEFrameUsesSemanticCommitment(t *testing.T) {
	tests := []struct {
		name     string
		frame    string
		kind     OpenAIHedgeEventKind
		terminal bool
	}{
		{name: "rate limits preamble", frame: "event: codex.rate_limits\ndata: {\"type\":\"codex.rate_limits\"}\n\n", kind: OpenAIHedgeEventPreamble},
		{name: "created preamble", frame: "data: {\"type\":\"response.created\"}\n\n", kind: OpenAIHedgeEventPreamble},
		{name: "reasoning", frame: "data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"x\"}\n\n", kind: OpenAIHedgeEventReasoning},
		{name: "text", frame: "data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n", kind: OpenAIHedgeEventText},
		{name: "tool", frame: "data: {\"type\":\"response.function_call_arguments.delta\",\"delta\":\"{}\"}\n\n", kind: OpenAIHedgeEventTool},
		{name: "success", frame: "data: {\"type\":\"response.completed\"}\n\n", kind: OpenAIHedgeEventTerminalSuccess, terminal: true},
		{name: "failure", frame: "data: {\"type\":\"response.failed\"}\n\n", kind: OpenAIHedgeEventTerminalFailure, terminal: true},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			event, terminal, err := classifyOpenAIHedgeSSEFrame([]byte(testCase.frame))
			require.NoError(t, err)
			require.Equal(t, testCase.kind, event.Kind)
			require.Equal(t, testCase.terminal, terminal)
			require.Equal(t, testCase.frame, string(event.Data))
		})
	}
}

func TestOpenAIHedgeAttemptWriterBuffersPartialFramesAndSuppressesDone(t *testing.T) {
	emitter := &openAIHedgeGatewayEmitter{}
	writer := newOpenAIHedgeAttemptWriter(context.Background(), func() {}, emitter)
	_, err := writer.WriteString("data: {\"type\":\"response.created\"}")
	require.NoError(t, err)
	writer.Flush()
	require.Empty(t, emitter.events)
	_, err = writer.WriteString("\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n")
	require.NoError(t, err)
	writer.Flush()
	require.Len(t, emitter.events, 2)
	require.Equal(t, OpenAIHedgeEventPreamble, emitter.events[0].Kind)
	require.Equal(t, OpenAIHedgeEventText, emitter.events[1].Kind)
	_, err = writer.WriteString("data: {\"type\":\"response.completed\"}\n\ndata: [DONE]\n\n")
	require.NoError(t, err)
	writer.Flush()
	require.Len(t, emitter.events, 2, "terminal waits until Forward returns usage/result")
	require.NoError(t, writer.finish(&OpenAIForwardResult{}, nil))
	require.Len(t, emitter.events, 3)
	require.Equal(t, OpenAIHedgeEventTerminalSuccess, emitter.events[2].Kind)
	require.True(t, writer.terminalEmitted())
}

func TestOpenAIHedgeCancelableUpstreamContextPreservesBaselineDetach(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	baseline, releaseBaseline := detachUpstreamContext(parent)
	defer releaseBaseline()
	cancelParent()
	require.NoError(t, baseline.Err(), "normal Forward must retain detached usage-drain behavior")

	parent, cancelParent = context.WithCancel(context.Background())
	cancelable, releaseCancelable := detachUpstreamContext(WithHedgeCancelableUpstreamContext(parent))
	releaseCancelable() // forwarding paths release immediately after request construction
	require.NoError(t, cancelable.Err(), "cleanup must not pre-cancel a hedge transport request")
	cancelParent()
	require.ErrorIs(t, cancelable.Err(), context.Canceled)
}

func TestCopyOpenAIHedgeAttemptContextPromotesOnlyWinnerKeys(t *testing.T) {
	destination, _ := gin.CreateTestContext(nil)
	winner, _ := gin.CreateTestContext(nil)
	loser, _ := gin.CreateTestContext(nil)
	destination.Set("shared", "destination")
	winner.Set("shared", "winner")
	winner.Set("winner_only", int64(2))
	loser.Set("loser_only", int64(1))

	CopyOpenAIHedgeAttemptContext(destination, winner)

	shared, exists := destination.Get("shared")
	require.True(t, exists)
	require.Equal(t, "winner", shared)
	winnerOnly, exists := destination.Get("winner_only")
	require.True(t, exists)
	require.Equal(t, int64(2), winnerOnly)
	_, exists = destination.Get("loser_only")
	require.False(t, exists)
}

func TestOpenAIHedgeClientSinkWritesOneSuccessfulTerminal(t *testing.T) {
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	sink := &OpenAIHedgeClientSink{Writer: ginContext.Writer}
	terminal := OpenAIHedgeOutput{Event: OpenAIHedgeEvent{
		Kind: OpenAIHedgeEventTerminalSuccess,
		Data: []byte("event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n"),
	}}

	require.NoError(t, sink.WriteOpenAIHedgeEvent(context.Background(), terminal))
	require.Equal(t, 1, strings.Count(recorder.Body.String(), "event: response.completed"))
	require.Equal(t, 1, strings.Count(recorder.Body.String(), "data: [DONE]"))
	require.ErrorIs(t, sink.WriteOpenAIHedgeEvent(context.Background(), terminal), ErrOpenAIHedgeEventAfterTerminal)
	require.Equal(t, 1, strings.Count(recorder.Body.String(), "data: [DONE]"))
}

func TestOpenAIHedgeClientSinkFailureDoesNotWriteDone(t *testing.T) {
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	sink := &OpenAIHedgeClientSink{Writer: ginContext.Writer}

	require.NoError(t, sink.WriteOpenAIHedgeEvent(context.Background(), OpenAIHedgeOutput{Event: OpenAIHedgeEvent{
		Kind: OpenAIHedgeEventTerminalFailure,
		Data: []byte("event: error\ndata: {\"type\":\"error\"}\n\n"),
	}}))
	require.NotContains(t, recorder.Body.String(), "data: [DONE]")
}

func TestBoundedOpenAIHedgeForwardErrorDoesNotLeakRawError(t *testing.T) {
	err := boundedOpenAIHedgeForwardError(errors.New("Authorization: Bearer secret-token"))
	require.EqualError(t, err, "openai hedge upstream failed")
}
