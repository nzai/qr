package main

import (
	"context"
	"fmt"
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

// Config 配置
type Config struct {
	ExchangeCodes   []string
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	MaxRetry        int
	Bucket          string
	Parallel        int
	QueueURL        string
}

// GetFromEnvironmentVariable 从环境变量中读取配置
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

	bucket := strings.TrimSpace(os.Getenv("Bucket"))
	if bucket == "" {
		return fmt.Errorf("Bucket invalid")
	}

	parallel, err := strconv.Atoi(strings.TrimSpace(os.Getenv("Parallel")))
	if err != nil {
		parallel = constants.DefaultParallel
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
	c.Bucket = bucket
	c.Parallel = parallel
	c.QueueURL = queueURL

	return nil
}

// Lister 查询公司列表
type Lister struct {
	config    *Config
	exchanges []exchanges.Exchange
	sqsClient *sqs.SQS
}

// NewLister 新建查询公司列表
func NewLister(config *Config, exchanges []exchanges.Exchange, sqsClient *sqs.SQS) *Lister {
	return &Lister{config, exchanges, sqsClient}
}

// Handler 处理逻辑
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
	// 交易所的昨天
	year, month, day := time.Now().In(exchange.Location()).AddDate(0, 0, -1).Date()
	date := time.Date(year, month, day, 0, 0, 0, 0, exchange.Location())

	// 获取上市公司
	companies, err := exchange.Companies()
	if err != nil {
		zap.L().Error("get exchange companies failed", zap.Error(err), zap.String("exchange", exchange.Code()), zap.Time("date", date))
		return
	}

	count := len(companies)
	zap.L().Info("list companies success",
		zap.String("exchange", exchange.Code()),
		zap.Time("date", date),
		zap.Int("companies", count),
		zap.Duration("elapsed", time.Now().Sub(start)))

	// 发送sqs消息
	failedCount, err := s.send2sqs(exchange, companies, date)
	if err != nil {
		zap.L().Error("send exchange daily company message failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date),
			zap.Int("companies", count))
		return
	}

	zap.L().Info("send exchange daily company message finished",
		zap.String("exchange", exchange.Code()),
		zap.Time("date", date),
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

// exchangeKey 交易所的key
func (s Lister) exchangeKey(exchange exchanges.Exchange, date time.Time) string {
	// {year}/{month}/{day}/{exchange}
	return fmt.Sprintf("%s/%s", date.Format("2006/01/02"), exchange.Code())
}
