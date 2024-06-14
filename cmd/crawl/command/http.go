package command

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/bytedance/sonic"
	"go.uber.org/zap"
)

var (
	ErrUnexpectedStatusCode = errors.New("unexpected status code")
)

func httpGet[T any](ctx context.Context, url string, body io.Reader) (T, error) {
	response := new(T)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, body)
	if err != nil {
		return *response, err
	}

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		zap.S().Warnw("failed to do http request", "error", err)
		return *response, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		zap.S().Warnw("unexpected status code", "statusCode", resp.StatusCode)
		return *response, ErrUnexpectedStatusCode
	}

	err = sonic.ConfigFastest.NewDecoder(resp.Body).Decode(response)
	if err != nil {
		zap.S().Warnw("failed to unmarshal response", "error", err)
		return *response, err
	}

	zap.S().Debugw("successfully get response", "response", response)

	return *response, nil
}
