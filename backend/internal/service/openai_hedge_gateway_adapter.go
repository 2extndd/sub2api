package service

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

var ErrOpenAIHedgeNativeFrame = errors.New("invalid OpenAI hedge native frame")

// OpenAIHedgeGatewayAttemptInput is request-scoped and never persisted. Each
// attempt receives its own Gin context and writer; only the coordinator sink can
// write winner bytes to the real client.
type OpenAIHedgeGatewayAttemptInput struct {
	Gateway        *OpenAIGatewayService
	Request        *http.Request
	GinKeys        map[string]any
	PrimaryAccount *Account
	HedgeAccount   *Account
	PrimaryBody    []byte
	HedgeBody      []byte
	HedgeInput     func() (*Account, []byte)
}

// OpenAIHedgeGatewayAttemptPort reuses the production Forward parser and
// transformations with attempt-local response writers.
type OpenAIHedgeGatewayAttemptPort struct {
	input OpenAIHedgeGatewayAttemptInput

	mu       sync.Mutex
	results  map[OpenAIAttemptRole]*OpenAIForwardResult
	contexts map[OpenAIAttemptRole]*gin.Context
	errors   map[OpenAIAttemptRole]error
}

func NewOpenAIHedgeGatewayAttemptPort(input OpenAIHedgeGatewayAttemptInput) (*OpenAIHedgeGatewayAttemptPort, error) {
	if input.Gateway == nil {
		return nil, errors.New("openai hedge gateway service is required")
	}
	if input.Request == nil {
		return nil, errors.New("openai hedge request snapshot is required")
	}
	if input.PrimaryAccount == nil {
		return nil, errors.New("openai hedge primary account is required")
	}
	return &OpenAIHedgeGatewayAttemptPort{
		input:    input,
		results:  make(map[OpenAIAttemptRole]*OpenAIForwardResult, 2),
		contexts: make(map[OpenAIAttemptRole]*gin.Context, 2),
		errors:   make(map[OpenAIAttemptRole]error, 2),
	}, nil
}

func (port *OpenAIHedgeGatewayAttemptPort) RunOpenAIHedgeAttempt(
	ctx context.Context,
	attempt OpenAIHedgeAttempt,
	emitter OpenAIHedgeEventEmitter,
) error {
	account, body, err := port.attemptInput(attempt.Role)
	if err != nil {
		return err
	}
	forwardContext, cancelForward := context.WithCancel(ctx)
	defer cancelForward()
	writer := newOpenAIHedgeAttemptWriter(forwardContext, cancelForward, emitter)
	attemptContext := newOpenAIHedgeAttemptGinContext(forwardContext, port.input.Request, port.input.GinKeys, writer)
	port.mu.Lock()
	port.contexts[attempt.Role] = attemptContext
	port.mu.Unlock()
	result, forwardErr := port.input.Gateway.Forward(WithHedgeCancelableUpstreamContext(forwardContext), attemptContext, account, body)
	port.mu.Lock()
	port.errors[attempt.Role] = forwardErr
	if result != nil {
		port.results[attempt.Role] = result
	}
	port.mu.Unlock()
	if err := writer.finish(result, forwardErr); err != nil {
		return err
	}
	return forwardErr
}

func (port *OpenAIHedgeGatewayAttemptPort) Result(role OpenAIAttemptRole) *OpenAIForwardResult {
	if port == nil {
		return nil
	}
	port.mu.Lock()
	defer port.mu.Unlock()
	return port.results[role]
}

func (port *OpenAIHedgeGatewayAttemptPort) Context(role OpenAIAttemptRole) *gin.Context {
	if port == nil {
		return nil
	}
	port.mu.Lock()
	defer port.mu.Unlock()
	return port.contexts[role]
}

func (port *OpenAIHedgeGatewayAttemptPort) Error(role OpenAIAttemptRole) error {
	if port == nil {
		return nil
	}
	port.mu.Lock()
	defer port.mu.Unlock()
	return port.errors[role]
}

func (port *OpenAIHedgeGatewayAttemptPort) attemptInput(role OpenAIAttemptRole) (*Account, []byte, error) {
	switch role {
	case OpenAIAttemptRolePrimary:
		return port.input.PrimaryAccount, append([]byte(nil), port.input.PrimaryBody...), nil
	case OpenAIAttemptRoleHedge:
		account, body := port.input.HedgeAccount, port.input.HedgeBody
		if port.input.HedgeInput != nil {
			account, body = port.input.HedgeInput()
		}
		if account == nil {
			return nil, nil, errors.New("openai hedge secondary account is unavailable")
		}
		return account, append([]byte(nil), body...), nil
	default:
		return nil, nil, fmt.Errorf("invalid openai hedge attempt role %d", role)
	}
}

func boundedOpenAIHedgeForwardError(err error) error {
	if err == nil {
		return errors.New("openai hedge upstream failed")
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return errors.New("openai hedge upstream failed")
}

func newOpenAIHedgeAttemptGinContext(
	ctx context.Context,
	request *http.Request,
	keys map[string]any,
	writer gin.ResponseWriter,
) *gin.Context {
	attempt := &gin.Context{Request: request.Clone(ctx), Writer: writer}
	attempt.Keys = make(map[string]any, len(keys))
	for key, value := range keys {
		attempt.Keys[key] = value
	}
	return attempt
}

// SnapshotOpenAIHedgeGinKeys copies request metadata before attempt goroutines
// start. Values are read-only domain pointers/scalars; each attempt gets its own
// map, so Forward's c.Set calls cannot race.
func SnapshotOpenAIHedgeGinKeys(c *gin.Context) map[string]any {
	if c == nil {
		return nil
	}
	// Called synchronously before attempt goroutines start. Gin Copy clones the
	// metadata map, after which each attempt clones it once more.
	copied := c.Copy()
	return copied.Keys
}

// CopyOpenAIHedgeAttemptContext promotes winner-only runtime marks (ops, cyber,
// passthrough) after both attempt goroutines have stopped. Loser state is never
// copied to the client request.
func CopyOpenAIHedgeAttemptContext(destination, winner *gin.Context) {
	if destination == nil || winner == nil {
		return
	}
	for key, value := range winner.Copy().Keys {
		destination.Set(key, value)
	}
}

// openAIHedgeAttemptWriter is a Gin writer backed only by an attempt-local
// parser. It emits complete transformed SSE frames; it never reaches the client.
type openAIHedgeAttemptWriter struct {
	ctx     context.Context
	cancel  context.CancelFunc
	emitter OpenAIHedgeEventEmitter
	header  http.Header

	mu              sync.Mutex
	status          int
	size            int
	buffer          bytes.Buffer
	terminal        bool
	pendingTerminal *OpenAIHedgeEvent
	flushErr        error
}

func newOpenAIHedgeAttemptWriter(ctx context.Context, cancel context.CancelFunc, emitter OpenAIHedgeEventEmitter) *openAIHedgeAttemptWriter {
	return &openAIHedgeAttemptWriter{ctx: ctx, cancel: cancel, emitter: emitter, header: make(http.Header), status: http.StatusOK, size: -1}
}

func (writer *openAIHedgeAttemptWriter) Header() http.Header { return writer.header }
func (writer *openAIHedgeAttemptWriter) snapshotHeader() http.Header {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	result := make(http.Header, len(writer.header))
	for key, values := range writer.header {
		result[key] = append([]string(nil), values...)
	}
	return result
}
func (writer *openAIHedgeAttemptWriter) WriteHeader(code int) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.size < 0 && code > 0 {
		writer.status = code
	}
}
func (writer *openAIHedgeAttemptWriter) WriteHeaderNow() {
	writer.mu.Lock()
	if writer.size < 0 {
		writer.size = 0
	}
	writer.mu.Unlock()
}
func (writer *openAIHedgeAttemptWriter) Write(data []byte) (int, error) {
	writer.mu.Lock()
	if writer.size < 0 {
		writer.size = 0
	}
	n, _ := writer.buffer.Write(data)
	writer.size += n
	writer.mu.Unlock()
	return n, nil
}
func (writer *openAIHedgeAttemptWriter) WriteString(value string) (int, error) {
	return writer.Write([]byte(value))
}
func (writer *openAIHedgeAttemptWriter) Status() int {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.status
}
func (writer *openAIHedgeAttemptWriter) Size() int {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.size
}
func (writer *openAIHedgeAttemptWriter) Written() bool { return writer.Size() >= 0 }
func (writer *openAIHedgeAttemptWriter) Flush() {
	if err := writer.flushFrames(false); err != nil {
		writer.mu.Lock()
		if writer.flushErr == nil {
			writer.flushErr = err
		}
		writer.mu.Unlock()
		if writer.cancel != nil {
			writer.cancel()
		}
	}
}
func (writer *openAIHedgeAttemptWriter) CloseNotify() <-chan bool {
	closed := make(chan bool)
	go func() {
		<-writer.ctx.Done()
		close(closed)
	}()
	return closed
}
func (writer *openAIHedgeAttemptWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return nil, nil, errors.New("openai hedge writer does not support hijacking")
}
func (writer *openAIHedgeAttemptWriter) Pusher() http.Pusher { return nil }

func (writer *openAIHedgeAttemptWriter) terminalEmitted() bool {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.terminal
}

func (writer *openAIHedgeAttemptWriter) finish(result *OpenAIForwardResult, forwardErr error) error {
	writer.mu.Lock()
	flushErr := writer.flushErr
	writer.mu.Unlock()
	if flushErr != nil {
		return flushErr
	}
	if err := writer.flushFrames(true); err != nil {
		return err
	}
	writer.mu.Lock()
	pending := writer.pendingTerminal
	writer.pendingTerminal = nil
	writer.mu.Unlock()
	var terminal OpenAIHedgeEvent
	switch {
	case pending != nil:
		terminal = cloneOpenAIHedgeEvent(*pending)
	case forwardErr != nil:
		terminal = OpenAIHedgeEvent{Kind: OpenAIHedgeEventTerminalFailure, Err: boundedOpenAIHedgeForwardError(forwardErr)}
	default:
		// Forward returns usage, not an exact provider cost. Keep billing unknown
		// so loser accounting conservatively uses PotentialCostMicros.
		terminal = OpenAIHedgeEvent{Kind: OpenAIHedgeEventTerminalSuccess}
	}
	if err := writer.emitter.EmitOpenAIHedgeEvent(terminal); err != nil {
		return err
	}
	writer.mu.Lock()
	writer.terminal = true
	writer.mu.Unlock()
	return nil
}

func (writer *openAIHedgeAttemptWriter) flushFrames(final bool) error {
	writer.mu.Lock()
	data := append([]byte(nil), writer.buffer.Bytes()...)
	cut := len(data)
	if !final {
		if index := bytes.LastIndex(data, []byte("\n\n")); index >= 0 {
			cut = index + 2
		} else {
			writer.mu.Unlock()
			return nil
		}
	}
	ready := data[:cut]
	remaining := append([]byte(nil), data[cut:]...)
	writer.buffer.Reset()
	_, _ = writer.buffer.Write(remaining)
	writer.mu.Unlock()

	for _, raw := range bytes.Split(ready, []byte("\n\n")) {
		if len(bytes.TrimSpace(raw)) == 0 {
			continue
		}
		event, terminal, err := classifyOpenAIHedgeSSEFrame(append(raw, '\n', '\n'))
		if err != nil {
			return err
		}
		if event.Kind == OpenAIHedgeEventUnknown {
			continue
		}
		if terminal {
			writer.mu.Lock()
			pending := cloneOpenAIHedgeEvent(event)
			writer.pendingTerminal = &pending
			writer.mu.Unlock()
			continue
		}
		if err := writer.emitter.EmitOpenAIHedgeEvent(event); err != nil {
			return err
		}
	}
	return nil
}

func classifyOpenAIHedgeSSEFrame(frame []byte) (OpenAIHedgeEvent, bool, error) {
	trimmed := bytes.TrimSpace(frame)
	if len(trimmed) == 0 || bytes.HasPrefix(trimmed, []byte(":")) {
		return OpenAIHedgeEvent{}, false, nil
	}
	parser := openAICompatSSEFrameParser{}
	var parsed openAICompatSSEFrame
	var ok bool
	for _, line := range strings.Split(string(trimmed), "\n") {
		if frameValue, dispatch := parser.AddLine(line); dispatch {
			parsed, ok = frameValue, true
		}
	}
	if !ok {
		if frameValue, dispatch := parser.Finish(); dispatch {
			parsed, ok = frameValue, true
		}
	}
	if !ok {
		return OpenAIHedgeEvent{}, false, ErrOpenAIHedgeNativeFrame
	}
	payload := strings.TrimSpace(parsed.Data)
	if payload == "[DONE]" {
		return OpenAIHedgeEvent{}, false, nil
	}
	eventType := strings.TrimSpace(gjson.Get(payload, "type").String())
	if eventType == "" {
		eventType = strings.TrimSpace(parsed.EventType)
	}
	kind := OpenAIHedgeEventPreamble
	terminal := false
	switch {
	case eventType == "codex.rate_limits", openAIStreamEventIsPreamble(eventType):
		kind = OpenAIHedgeEventPreamble
	case strings.Contains(eventType, "reasoning"):
		kind = OpenAIHedgeEventReasoning
	case strings.Contains(eventType, "function_call"), strings.Contains(eventType, "tool_call"):
		kind = OpenAIHedgeEventTool
	case eventType == "response.failed", eventType == "response.incomplete", eventType == "response.cancelled", eventType == "response.canceled":
		kind, terminal = OpenAIHedgeEventTerminalFailure, true
	case eventType == "response.completed", eventType == "response.done":
		kind, terminal = OpenAIHedgeEventTerminalSuccess, true
	case openAIStreamDataStartsClientOutput(payload, eventType):
		kind = OpenAIHedgeEventText
	default:
		kind = OpenAIHedgeEventPreamble
	}
	event := OpenAIHedgeEvent{Kind: kind, Data: append([]byte(nil), frame...)}
	if kind == OpenAIHedgeEventTerminalFailure {
		event.Err = errors.New("openai hedge upstream terminal failure")
	}
	return event, terminal, nil
}

// OpenAIHedgeClientSink serializes committed winner frames to one client.
type OpenAIHedgeClientSink struct {
	Writer   gin.ResponseWriter
	Attempts *OpenAIHedgeGatewayAttemptPort

	mu              sync.Mutex
	headersWritten  bool
	terminalWritten bool
}

func (sink *OpenAIHedgeClientSink) WriteOpenAIHedgeEvent(_ context.Context, output OpenAIHedgeOutput) error {
	if sink == nil || sink.Writer == nil {
		return errors.New("openai hedge client sink is unavailable")
	}
	sink.mu.Lock()
	if !sink.headersWritten {
		if sink.Attempts != nil {
			if attemptContext := sink.Attempts.Context(output.Role); attemptContext != nil && attemptContext.Writer != nil {
				headers := attemptContext.Writer.Header()
				if attemptWriter, ok := attemptContext.Writer.(*openAIHedgeAttemptWriter); ok {
					headers = attemptWriter.snapshotHeader()
				}
				for key, values := range headers {
					for _, value := range values {
						sink.Writer.Header().Add(key, value)
					}
				}
			}
		}
		sink.headersWritten = true
	}
	if output.Event.Kind.terminal() {
		if sink.terminalWritten {
			sink.mu.Unlock()
			return ErrOpenAIHedgeEventAfterTerminal
		}
		sink.terminalWritten = true
	}
	if len(output.Event.Data) > 0 {
		if _, err := sink.Writer.Write(output.Event.Data); err != nil {
			sink.mu.Unlock()
			return err
		}
	}
	if output.Event.Kind == OpenAIHedgeEventTerminalSuccess {
		if _, err := sink.Writer.Write([]byte("data: [DONE]\n\n")); err != nil {
			sink.mu.Unlock()
			return err
		}
	}
	if len(output.Event.Data) > 0 || output.Event.Kind == OpenAIHedgeEventTerminalSuccess {
		sink.Writer.Flush()
	}
	sink.mu.Unlock()
	return nil
}

// OpenAIHedgeExposureRecorder records provider-cost exposure only. Persistence
// is supplied by the runtime telemetry adapter; this implementation never bills
// a user.
type OpenAIHedgeExposureRecorder struct {
	mu      sync.Mutex
	Records []OpenAIHedgeLoserBilling
}

func (recorder *OpenAIHedgeExposureRecorder) FinalizeOpenAIHedgeLoserBilling(_ context.Context, billing OpenAIHedgeLoserBilling) error {
	recorder.mu.Lock()
	recorder.Records = append(recorder.Records, billing)
	recorder.mu.Unlock()
	return nil
}

var _ gin.ResponseWriter = (*openAIHedgeAttemptWriter)(nil)
var _ OpenAIHedgeAttemptPort = (*OpenAIHedgeGatewayAttemptPort)(nil)
var _ OpenAIHedgeSink = (*OpenAIHedgeClientSink)(nil)
var _ OpenAIHedgeBillingPort = (*OpenAIHedgeExposureRecorder)(nil)
var _ io.Writer = (*openAIHedgeAttemptWriter)(nil)
