package service

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadOpenAICompatBufferedTerminalAcceptsObjectFunctionArguments(t *testing.T) {
	upstreamBody := strings.Join([]string{
		`data: {"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","id":"item_1","call_id":"call_1","name":"exec_command","arguments":{"cmd":"echo hi"},"status":"completed"}}`,
		`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"function_call","id":"item_1","call_id":"call_1","name":"exec_command","arguments":{"cmd":"echo hi"},"status":"completed"}],"usage":{"input_tokens":7,"output_tokens":3,"total_tokens":10}}}`,
		"",
	}, "\n\n")
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}

	result, usage, _, err := (&OpenAIGatewayService{}).readOpenAICompatBufferedTerminal(resp, nil, "test", "request_1")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Output, 1)
	require.JSONEq(t, `{"cmd":"echo hi"}`, result.Output[0].Arguments)
	require.Equal(t, 7, usage.InputTokens)
	require.Equal(t, 3, usage.OutputTokens)
}
