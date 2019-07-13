package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/nzai/qr/config"
	"github.com/nzai/qr/constants"
)

var (
	_wechatInstance *WeChatService
	_wechatOnce     sync.Once
)

// WeChatService wechat service
type WeChatService struct {
	accessTokenMutex    *sync.RWMutex
	accessTokenExpireAt int64
	accessToken         string
}

// GetWeChatService get wechat service instance
func GetWeChatService() *WeChatService {
	_wechatOnce.Do(func() {
		_wechatInstance = &WeChatService{
			accessTokenMutex:    new(sync.RWMutex),
			accessTokenExpireAt: -1,
		}
	})

	return _wechatInstance
}

// GetAccessToken return fresh access token
func (s WeChatService) GetAccessToken() (string, error) {
	s.accessTokenMutex.RLock()
	if s.accessTokenExpireAt > time.Now().Unix() {
		s.accessTokenMutex.RUnlock()
		return s.accessToken, nil
	}
	s.accessTokenMutex.RUnlock()

	err := s.refreshAccessToken()
	if err != nil {
		return "", err
	}

	return s.accessToken, nil
}

func (s *WeChatService) refreshAccessToken() error {
	s.accessTokenMutex.Lock()
	defer s.accessTokenMutex.Unlock()

	url := fmt.Sprintf("https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=%s&corpsecret=%s", config.Get().WeChat.CorpID, config.Get().WeChat.AppSecret)
	_, buffer, err := TryDownloadBytes(url, constants.RetryCount, constants.RetryInterval)
	if err != nil {
		zap.L().Error("get access token failed", zap.Error(err))
		return err
	}

	response := new(GetAccessTokenResponse)
	err = json.Unmarshal(buffer, response)
	if err != nil {
		zap.L().Error("unmarshal get access token response failed", zap.Error(err), zap.ByteString("buffer", buffer))
		return err
	}

	if response.ErrorCode != 0 {
		zap.L().Error("get access token return error code", zap.Int("code", response.ErrorCode), zap.String("message", response.ErrorMessage))
		return fmt.Errorf("get access token failed due to %s(%d)", response.ErrorMessage, response.ErrorCode)
	}

	s.accessToken = response.AccessToken
	s.accessTokenExpireAt = time.Now().Unix() + response.ExpiresIn

	zap.L().Debug("refresh access token success", zap.String("access token", s.accessToken))

	return nil
}

// GetAccessTokenResponse response of get access token request
type GetAccessTokenResponse struct {
	ErrorCode    int    `json:"errcode"`
	ErrorMessage string `json:"errmsg"`
	AccessToken  string `json:"access_token"`
	ExpiresIn    int64  `json:"expires_in"`
}

// SendMessage send wechat message
func (s WeChatService) SendMessage(message string) error {
	accessToken, err := s.GetAccessToken()
	if err != nil {
		return err
	}

	buffer, err := json.Marshal(&WeChatSendMessageRequest{
		ToUser:      "@all",
		MessageType: "text",
		AgentID:     config.Get().WeChat.AppID,
		Text: WeChatMessageText{
			Content: message,
		},
		Safe: 0,
	})
	if err != nil {
		zap.L().Error("marshal send message body failed", zap.Error(err))
		return err
	}

	url := fmt.Sprintf("https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token=%s", accessToken)
	response, err := http.DefaultClient.Post(url, "application/json", bytes.NewBuffer(buffer))
	if err != nil {
		zap.L().Error("post message failed", zap.Error(err), zap.String("url", url))
		return err
	}
	defer response.Body.Close()

	buffer, err = ioutil.ReadAll(response.Body)
	if err != nil {
		zap.L().Error("read send messag response failed", zap.Error(err), zap.String("url", url))
		return err
	}

	resp := new(WeChatSendMessageResponse)
	err = json.Unmarshal(buffer, resp)
	if err != nil {
		zap.L().Error("unmarshal send message response failed",
			zap.Error(err),
			zap.ByteString("response", buffer),
			zap.String("url", url))
		return err
	}

	if resp.ErrorCode != 0 {
		zap.L().Error("send message failed", zap.Any("response", resp), zap.String("url", url))
		return fmt.Errorf("send message failed due to %s", resp.ErrorMessage)
	}

	return nil
}

// WeChatSendMessageRequest wechat send message request
type WeChatSendMessageRequest struct {
	ToUser      string            `json:"touser"`
	MessageType string            `json:"msgtype"`
	AgentID     int               `json:"agentid"`
	Text        WeChatMessageText `json:"text"`
	Safe        int               `json:"safe"`
}

// WeChatMessageText wechat message text
type WeChatMessageText struct {
	Content string `json:"content"`
}

// WeChatSendMessageResponse wechat send message response
type WeChatSendMessageResponse struct {
	ErrorCode    int    `json:"errcode"`
	ErrorMessage string `json:"errmsg"`
	InvalidUser  string `json:"invaliduser"`
	InvalidParty string `json:"invalidparty"`
	InvalidTag   string `json:"invalidtag"`
}
