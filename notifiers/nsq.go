package notifiers

import (
	"crypto/tls"
	"encoding/json"

	"github.com/nsqio/go-nsq"
	"go.uber.org/zap"
)

// Nsq notify by nsq
type Nsq struct {
	topic    string
	producer *nsq.Producer
}

// NewNsq create new nsq notifier
func NewNsq(broker, tlsCert, tlsKey, topic string) Notifier {
	cert, err := tls.LoadX509KeyPair(tlsCert, tlsKey)
	if err != nil {
		zap.L().Fatal("init tls certificate failed",
			zap.Error(err),
			zap.String("tlsCert", tlsCert),
			zap.String("tlsKey", tlsKey))
	}

	config := nsq.NewConfig()
	config.TlsV1 = true
	config.TlsConfig = &tls.Config{
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{cert},
	}

	producer, err := nsq.NewProducer(broker, config)
	if err != nil {
		zap.L().Fatal("init nsq producer failed",
			zap.Error(err),
			zap.String("broker", broker))
	}

	return &Nsq{topic: topic, producer: producer}
}

// Notify notify exchange daily job result
func (s Nsq) Notify(result *ExchangeDailyJobResult) {
	buffer, err := json.Marshal(result)
	if err != nil {
		zap.L().Warn("marshal exchange daily job result failed",
			zap.Error(err),
			zap.Any("result", result))
		return
	}

	err = s.producer.Publish(s.topic, buffer)
	if err != nil {
		zap.L().Warn("publish exchange daily job result failed",
			zap.Error(err),
			zap.String("topic", s.topic),
			zap.Any("result", result))
		return
	}

	zap.L().Info("publish exchange daily job result success",
		zap.String("topic", s.topic),
		zap.Any("result", result))
}

// Close close producer
func (s Nsq) Close() {
	if s.producer == nil {
		return
	}

	s.producer.Stop()
}
