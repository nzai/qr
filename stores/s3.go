package stores

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/quotes"
	"go.uber.org/zap"
)

// S3Config aws s3 store config
type S3Config struct {
	AccessKeyID     string `yaml:"id"`
	SecretAccessKey string `yaml:"secret"`
	Region          string `yaml:"region"`
	Bucket          string `yaml:"bucket"`
}

// S3 define tencent cos store
type S3 struct {
	config *S3Config
	client *s3.S3
}

// NewS3 create tencent cos store
func NewS3(config *S3Config) *S3 {
	credential := credentials.NewStaticCredentialsFromCreds(credentials.Value{
		AccessKeyID:     config.AccessKeyID,
		SecretAccessKey: config.SecretAccessKey,
	})

	conf := aws.Config{
		Credentials: credential,
		Region:      aws.String(config.Region),
		MaxRetries:  aws.Int(5),
	}

	sess, err := session.NewSession(&conf)
	if err != nil {
		zap.L().Panic("create aws session failed", zap.Error(err), zap.Any("config", config))
	}

	return &S3{
		config: config,
		client: s3.New(sess),
	}
}

// storePath return store path
func (s S3) storePath(exchange exchanges.Exchange, date time.Time) string {
	return fmt.Sprintf("%s/%s", date.Format("2006/01/02"), exchange.Code())
}

// Exists check quote exists
func (s S3) Exists(exchange exchanges.Exchange, date time.Time) (bool, error) {
	_, err := s.client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(s.storePath(exchange, date)),
	})
	if err == nil {
		return true, nil
	}

	ae, ok := err.(awserr.Error)
	if ok && ae.Code() == "NotFound" {
		return false, nil
	}

	zap.L().Error("check exchange daily quote exists failed",
		zap.Error(err),
		zap.String("exchange", exchange.Code()),
		zap.Time("date", date))

	return false, err
}

// Save save exchange daily quote
func (s S3) Save(exchange exchanges.Exchange, date time.Time, edq *quotes.ExchangeDailyQuote) error {
	buffer := new(bytes.Buffer)
	// init gzip writer
	gw, err := gzip.NewWriterLevel(buffer, gzip.BestCompression)
	if err != nil {
		zap.L().Error("create gzip writer failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	// encode to gzip writer
	err = edq.Encode(gw)
	if err != nil {
		zap.L().Error("encode quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	gw.Flush()
	gw.Close()

	zipped, err := ioutil.ReadAll(buffer)
	if err != nil {
		return err
	}

	_, err = s.client.PutObject(&s3.PutObjectInput{
		Bucket:       aws.String(s.config.Bucket),
		Key:          aws.String(s.storePath(exchange, date)),
		Body:         bytes.NewReader(zipped),
		StorageClass: aws.String(s3.ObjectStorageClassReducedRedundancy),
	})
	if err != nil {
		zap.L().Error("put exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	return nil
}

// Load load exchange daily quote
func (s S3) Load(exchange exchanges.Exchange, date time.Time) (*quotes.ExchangeDailyQuote, error) {
	response, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(s.storePath(exchange, date)),
	})
	if err != nil {
		zap.L().Error("get exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return nil, err
	}
	defer response.Body.Close()

	// init gzip reader
	gr, err := gzip.NewReader(response.Body)
	if err != nil {
		zap.L().Error("create gzip reader failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return nil, err
	}
	defer gr.Close()

	buffer := new(bytes.Buffer)
	_, err = io.Copy(buffer, gr)
	if err != nil {
		zap.L().Error("get exchange daily quote response failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return nil, err
	}

	// decode from bytes
	edq := new(quotes.ExchangeDailyQuote)
	err = edq.Decode(buffer)
	if err != nil {
		zap.L().Error("decode exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return nil, err
	}

	return edq, nil
}

// Delete delete exchange daily quote
func (s S3) Delete(exchange exchanges.Exchange, date time.Time) error {
	key := s.storePath(exchange, date)
	_, err := s.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		zap.L().Error("delete exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date),
			zap.String("key", key))
		return err
	}

	return nil
}

// Close close store
func (s S3) Close() error {
	return nil
}
