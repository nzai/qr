package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nzai/qr/utils"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/messages"
	"github.com/nzai/qr/quotes"
	"github.com/vmihailenco/msgpack"
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

	var exs []exchanges.Exchange
	for _, exchangeCode := range config.ExchangeCodes {
		exchange, found := exchanges.Get(exchangeCode)
		if !found {
			zap.L().Fatal("exchange not found", zap.Error(err), zap.String("exchange code", exchangeCode))
		}

		exs = append(exs, exchange)
	}
	zap.L().Info("get exchanges success", zap.Strings("exchange codes", config.ExchangeCodes))

	list := NewLister(config, exs, sqs.New(awsSession))
	lambda.Start(list.Handler)
}

// Config define lambda config
type Config struct {
	ExchangeCodes   []string
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	MaxRetry        int
	QueueURL        string
}

// GetFromEnvironmentVariable read config from environment variables
func (c *Config) GetFromEnvironmentVariable() error {

	exchangeCodes := strings.TrimSpace(os.Getenv("ExchangeCodes"))
	if exchangeCodes == "" {
		return fmt.Errorf("exchange code invalid")
	}

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

	queueURL := strings.TrimSpace(os.Getenv("QueueUrl"))
	if queueURL == "" {
		return fmt.Errorf("QueueUrl invalid")
	}

	c.ExchangeCodes = strings.Split(exchangeCodes, ",")
	c.AccessKeyID = accessKeyID
	c.SecretAccessKey = secretAccessKey
	c.Region = region
	c.MaxRetry = maxRetry
	c.QueueURL = queueURL

	return nil
}

// Lister define list service
type Lister struct {
	config    *Config
	exchanges []exchanges.Exchange
	sqsClient *sqs.SQS
}

// NewLister create list service
func NewLister(config *Config, exchanges []exchanges.Exchange, sqsClient *sqs.SQS) *Lister {
	return &Lister{config, exchanges, sqsClient}
}

// Handler process lambda event
func (s Lister) Handler(ctx context.Context, event events.CloudWatchEvent) {
	wg := new(sync.WaitGroup)
	wg.Add(len(s.exchanges))
	for _, exchange := range s.exchanges {
		go s.listExchangeCompanies(exchange, wg)
	}
	wg.Wait()
}

func (s Lister) listExchangeCompanies(exchange exchanges.Exchange, wg *sync.WaitGroup) {
	defer wg.Done()

	start := time.Now()
	yesterday := utils.YesterdayZero(start.In(exchange.Location()))

	// 获取上市公司
	companies, err := exchange.Companies()
	if err != nil {
		zap.L().Error("get exchange companies failed", zap.Error(err), zap.String("exchange", exchange.Code()), zap.Time("date", yesterday))
		return
	}

	count := len(companies)
	zap.L().Info("list companies success",
		zap.String("exchange", exchange.Code()),
		zap.Time("date", yesterday),
		zap.Int("companies", count),
		zap.Duration("elapsed", time.Now().Sub(start)))

	// 发送sqs消息
	failedCount, err := s.send2sqs(exchange, companies, yesterday)
	if err != nil {
		zap.L().Error("send exchange daily company message failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", yesterday),
			zap.Int("companies", count))
		return
	}

	zap.L().Info("send exchange daily company message finished",
		zap.String("exchange", exchange.Code()),
		zap.Time("date", yesterday),
		zap.Int("companies", count),
		zap.Int("success", count-failedCount),
		zap.Int("failed", failedCount),
		zap.Duration("elapsed", time.Now().Sub(start)))
}

func (s Lister) send2sqs(exchange exchanges.Exchange, companies map[string]*quotes.Company, date time.Time) (int, error) {
	input := &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(s.config.QueueURL),
		Entries:  make([]*sqs.SendMessageBatchRequestEntry, 0, constants.AwsSqsMaxBatchSize),
	}

	var failedCount int
	index := -1
	for code, company := range companies {
		index++
		body, err := msgpack.Marshal(&messages.CompanyDaily{
			Exchange: exchange.Code(),
			Company:  company,
			Date:     date,
		})
		if err != nil {
			zap.L().Error("marshal company daily message failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", company),
				zap.Time("date", date))
			return 0, err
		}

		input.Entries = append(input.Entries, &sqs.SendMessageBatchRequestEntry{
			Id:          aws.String(fmt.Sprintf("%s:%s", exchange.Code(), code)),
			MessageBody: aws.String(string(body)),
		})

		if len(input.Entries) != constants.AwsSqsMaxBatchSize && index < len(companies)-1 {
			continue
		}

		output, err := s.sqsClient.SendMessageBatch(input)
		if err != nil {
			zap.L().Error("batch send company daily messages failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", company))
			return 0, err
		}

		for _, failed := range output.Failed {
			failedCount++
			zap.L().Error("send company daily message failed",
				zap.String("error", failed.GoString()),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", company))
		}

		// clear all
		input.Entries = input.Entries[:0]
	}

	return failedCount, nil
}
