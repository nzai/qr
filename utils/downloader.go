package utils

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// TryDownloadString try download string by url
func TryDownloadString(url string, retry int, retryInterval time.Duration) (string, error) {
	return TryDownloadStringWithHeader(url, nil, retry, retryInterval)
}

// TryDownloadStringWithHeader try download string by url
func TryDownloadStringWithHeader(url string, headers map[string]string, retry int, retryInterval time.Duration) (string, error) {
	code, buffer, err := TryDownloadBytesWithHeader(url, headers, retry, retryInterval)
	if err != nil {
		return "", err
	}

	if code != http.StatusOK {
		zap.L().Warn("unexpected response status", zap.Int("code", code))
		return "", fmt.Errorf("unexpected response status (%d)%s", code, http.StatusText(code))
	}

	return string(buffer), nil
}

// TryDownloadBytes try download bytes by url
func TryDownloadBytes(url string, retry int, retryInterval time.Duration) (int, []byte, error) {
	return TryDownloadBytesWithHeader(url, nil, retry, retryInterval)
}

// TryDownloadBytesWithHeader try download bytes by url
func TryDownloadBytesWithHeader(url string, headers map[string]string, retry int, retryInterval time.Duration) (int, []byte, error) {
	var code int
	var buffer []byte
	var err error
	for index := 0; index < retry; index++ {
		code, buffer, err = tryDownloadBytesOnce(url, headers)
		if err == nil && code == http.StatusOK {
			return code, buffer, nil
		}

		if index < retry-1 {
			time.Sleep(retryInterval)
		}
	}

	return code, buffer, err
}

func tryDownloadBytesOnce(url string, headers map[string]string) (int, []byte, error) {
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		zap.L().Warn("create http request failed", zap.Error(err), zap.String("url", url))
		return 0, nil, err
	}

	for key, value := range headers {
		request.Header.Set(key, value)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		// zap.L().Warn("do http request failed", zap.Error(err), zap.String("url", url))
		return 0, nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return response.StatusCode, nil, nil
	}

	buffer, err := ioutil.ReadAll(response.Body)
	if err != nil {
		zap.L().Warn("read http response body failed", zap.Error(err), zap.String("url", url))
		return 0, nil, err
	}

	return response.StatusCode, buffer, nil
}
