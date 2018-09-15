package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/messages"
	"github.com/nzai/qr/quotes"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	config := new(Config)
	err := config.GetFromEnvironmentVariable()
	if err != nil {
		zap.L().Fatal("get environment variables failed", zap.Error(err))
	}
	zap.L().Info("get environment variables success")

	creds := credentials.NewStaticCredentialsFromCreds(credentials.Value{AccessKeyID: config.AccessKeyID, SecretAccessKey: config.SecretAccessKey})
	awsConfig := aws.NewConfig().WithCredentials(creds).WithRegion(config.Region).WithMaxRetries(config.MaxRetry)

	awsSession, err := session.NewSession(awsConfig)
	if err != nil {
		zap.L().Fatal("new aws session failed", zap.Error(err))
	}

	crawl := NewCrawler(config, s3.New(awsSession), sqs.New(awsSession))
	lambda.Start(crawl.Handler)
}

// Config define lambda config
type Config struct {
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	MaxRetry        int
	Bucket          string
	QueueURL        string
}

// GetFromEnvironmentVariable read config from environment variables
func (c *Config) GetFromEnvironmentVariable() error {
	accessKeyID := strings.TrimSpace(os.Getenv("AccessKeyID"))
	if accessKeyID == "" {
		return fmt.Errorf("AccessKeyID invalid")
	}

	secretAccessKey := strings.TrimSpace(os.Getenv("SecretAccessKey"))
	if secretAccessKey == "" {
		return fmt.Errorf("SecretAccessKey invalid")
	}

	region := strings.TrimSpace(os.Getenv("Region"))
	if region == "" {
		return fmt.Errorf("Region invalid")
	}

	maxRetry, err := strconv.Atoi(strings.TrimSpace(os.Getenv("MaxRetry")))
	if err != nil {
		maxRetry = constants.RetryCount
	}

	bucket := strings.TrimSpace(os.Getenv("Bucket"))
	if bucket == "" {
		return fmt.Errorf("Bucket invalid")
	}

	queueURL := strings.TrimSpace(os.Getenv("QueueUrl"))
	if queueURL == "" {
		return fmt.Errorf("QueueUrl invalid")
	}

	c.AccessKeyID = accessKeyID
	c.SecretAccessKey = secretAccessKey
	c.Region = region
	c.MaxRetry = maxRetry
	c.Bucket = bucket
	c.QueueURL = queueURL

	return nil
}

// Crawler define crawl service
type Crawler struct {
	config    *Config
	client    *s3.S3
	sqsClient *sqs.SQS
}

// NewCrawler create crawl service
func NewCrawler(config *Config, client *s3.S3, sqsClient *sqs.SQS) *Crawler {
	return &Crawler{config, client, sqsClient}
}

// Handler process sqs event
func (s Crawler) Handler(ctx context.Context, event events.SQSEvent) {
	wg := new(sync.WaitGroup)
	wg.Add(len(event.Records))
	for _, record := range event.Records {
		// 并行处理
		go s.onMessage(ctx, record, wg)
	}
	wg.Wait()

	zap.L().Info("crawl success", zap.Int("records", len(event.Records)))
}

// onMessage fire on message arrived
func (s Crawler) onMessage(ctx context.Context, message events.SQSMessage, wg *sync.WaitGroup) {
	defer wg.Done()

	zap.L().Debug("receive message", zap.Any("message", message))
	// parse
	exchange, company, date, err := s.parseMessage(message)
	if err != nil {
		zap.L().Error("parse message failed", zap.Error(err), zap.Any("message", message))
		return
	}

	// check exists
	key := s.s3Key(exchange, company, date)
	_, err = s.client.HeadObject(&s3.HeadObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(key)})
	if err == nil {
		// already exists
		return
	}

	// crawl company daily quote
	err = s.crawl(exchange, company, key, date)
	if err != nil {
		zap.L().Error("crawl company daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.String("bucket", s.config.Bucket),
			zap.String("key", key))
		return
	}

	// delete message
	_, err = s.sqsClient.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.config.QueueURL),
		ReceiptHandle: aws.String(message.ReceiptHandle),
	})
	if err != nil {
		zap.L().Error("delete company daily message failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.String("queue", s.config.QueueURL),
			zap.String("id", message.MessageId))
		return
	}
}

// parseMessage parse message body
func (s Crawler) parseMessage(message events.SQSMessage) (exchanges.Exchange, *quotes.Company, time.Time, error) {
	var date time.Time

	companyDaily := new(messages.CompanyDaily)
	err := json.Unmarshal([]byte(message.Body), companyDaily)
	if err != nil {
		zap.L().Error("unmarshal company daily message failed", zap.Error(err), zap.Any("message", message))
		return nil, nil, date, err
	}

	exchange, found := exchanges.Get(companyDaily.Exchange)
	if !found {
		return nil, nil, date, errors.New("exchange invalid")
	}

	return exchange, companyDaily.Company, companyDaily.Date, nil
}

// s3Key define s3 save path
func (s Crawler) s3Key(exchange exchanges.Exchange, company *quotes.Company, date time.Time) string {
	// {year}/{month}/{day}/{exchange}/{company}
	return fmt.Sprintf("%s/%s/%s", date.Format("2006/01/02"), exchange.Code(), company.Code)
}

// crawl company daily quote and save
func (s Crawler) crawl(exchange exchanges.Exchange, company *quotes.Company, key string, date time.Time) error {
	// crawl
	cdq, err := exchange.Crawl(company, date)
	if err != nil {
		// just warn and return without error
		zap.L().Warn("crawl company daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date))
		return nil
	}

	if cdq.IsEmpty() {
		// just warn and return without error
		zap.L().Warn("company daily quote is empty",
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date))
		return nil
	}

	buffer := new(bytes.Buffer)
	err = cdq.Encode(buffer)
	if err != nil {
		zap.L().Error("encode company daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date))
		return err
	}

	// save
	_, err = s.client.PutObject(&s3.PutObjectInput{
		Bucket:       aws.String(s.config.Bucket),
		Key:          aws.String(key),
		Body:         bytes.NewReader(buffer.Bytes()),
		StorageClass: aws.String(s3.ObjectStorageClassStandard),
	})
	if err != nil {
		zap.L().Error("save company daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.String("bucket", s.config.Bucket),
			zap.String("key", key),
			zap.Int("size", buffer.Len()))
		return err
	}

	// load
	output, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		zap.L().Error("load company daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.String("bucket", s.config.Bucket),
			zap.String("key", key))
		return err
	}
	defer output.Body.Close()

	buffer.Reset()
	_, err = io.Copy(buffer, output.Body)
	if err != nil {
		zap.L().Error("read saved company daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.String("bucket", s.config.Bucket),
			zap.String("key", key))
		return err
	}

	saved := new(quotes.CompanyDailyQuote)
	err = saved.Decode(buffer)
	if err != nil {
		zap.L().Error("decode saved company daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.String("bucket", s.config.Bucket),
			zap.String("key", key))
		return err
	}

	// validate
	err = cdq.Equal(*saved)
	if err != nil {
		zap.L().Error("company daily quote is different from saved",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.String("bucket", s.config.Bucket),
			zap.String("key", key))

		// delete saved
		_, err1 := s.client.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(s.config.Bucket),
			Key:    aws.String(key),
		})
		if err1 != nil {
			zap.L().Error("delete saved company daily quote failed",
				zap.Error(err1),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", company),
				zap.Time("date", date),
				zap.String("bucket", s.config.Bucket),
				zap.String("key", key))
		}

		return err
	}

	zap.L().Info("save company daily quote success",
		zap.String("exchange", exchange.Code()),
		zap.Any("company", company),
		zap.Time("date", date),
		zap.String("bucket", s.config.Bucket),
		zap.String("key", key),
		zap.Int("pre", len(*cdq.Pre)),
		zap.Int("regular", len(*cdq.Regular)),
		zap.Int("post", len(*cdq.Post)),
	)

	return nil
}
