package config

import (
	"errors"
	"strings"

	"github.com/nzai/qr/constants"

	"github.com/BurntSushi/toml"
)

// Config global config
type Config struct {
	Exchanges string `toml:"exchanges"`
	Stores    string `toml:"stores"`
	LastDays  int    `toml:"last_days"`
	WeChat    struct {
		CorpID    string `toml:"corp_id"`
		AppID     int    `toml:"app_id"`
		AppSecret string `toml:"app_secret"`
	} `toml:"wechat"`
	// Nsq struct {
	// 	Broker  string `toml:"broker"`
	// 	TLSCert string `toml:"tls_cert"`
	// 	TLSKey  string `toml:"tls_key"`
	// 	Topic   string `toml:"topic"`
	// } `toml:"nsq"`
}

// Valid validate config
func (s Config) Valid() error {
	if strings.TrimSpace(s.Exchanges) == "" {
		return errors.New("exchanges undefined")
	}

	if strings.TrimSpace(s.Stores) == "" {
		return errors.New("stores undefined")
	}

	if s.LastDays <= 0 {
		s.LastDays = constants.DefaultLastDays
	}

	if strings.TrimSpace(s.WeChat.CorpID) == "" {
		return errors.New("wechat.corp_id undefined")
	}

	if s.WeChat.AppID == 0 {
		return errors.New("wechat.app_id undefined")
	}

	if strings.TrimSpace(s.WeChat.AppSecret) == "" {
		return errors.New("wechat.app_secret undefined")
	}

	// if strings.TrimSpace(s.Nsq.Broker) == "" {
	// 	return errors.New("nsq.broker undefined")
	// }

	// if strings.TrimSpace(s.Nsq.TLSCert) == "" {
	// 	return errors.New("nsq.tls_cert undefined")
	// }

	// _, err := os.Stat(s.Nsq.TLSCert)
	// if os.IsNotExist(err) {
	// 	return errors.New("nsq.tls_cert not exist")
	// }

	// if strings.TrimSpace(s.Nsq.TLSKey) == "" {
	// 	return errors.New("nsq.tls_key undefined")
	// }

	// _, err = os.Stat(s.Nsq.TLSKey)
	// if os.IsNotExist(err) {
	// 	return errors.New("nsq.tls_key not exist")
	// }

	// if strings.TrimSpace(s.Nsq.Topic) == "" {
	// 	return errors.New("nsq.topic undefined")
	// }

	return nil
}

var (
	currentConfig *Config
)

// Get get current config
func Get() *Config {
	return currentConfig
}

// Parse parse config from file
func Parse(filePath string) (*Config, error) {
	currentConfig = new(Config)
	_, err := toml.DecodeFile(filePath, currentConfig)
	if err != nil {
		return nil, err
	}

	return currentConfig, currentConfig.Valid()
}
