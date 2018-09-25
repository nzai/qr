package stores

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/quotes"
	"go.uber.org/zap"
)

// exchange daily				key: et:{exchange}:{date}												value:1 / 0 (is trading day)
// exchange daily companies		key: ec:{exchange}:{date}:{companyCode}									value:{companyName}
// company daily rollup quote	key: 1d:{exchange}:{companyCode}:{date} 								value:{open},{close},{high},{low},{volume}
// company daily quote serial	key: 1m:{exchange}:{companyCode}:{date}:{Pre|Regular|Post}:{timestamp}	value:{open},{close},{high},{low},{volume}
// company daily dividend		key: dividend:{exchange}:{companyCode}:{date}							value:{timestamp},{amount}
// company daily split			key: split:{exchange}:{companyCode}:{date}								value:{timestamp},{numerator},{denominator}

// Redis define redis store
type Redis struct {
	client *redis.Client
}

// NewRedis create redis store
func NewRedis(address, password string) *Redis {
	client := redis.NewClient(&redis.Options{
		Addr:         address,
		Password:     password,
		DB:           0, // use default DB
		MaxRetries:   2,
		ReadTimeout:  time.Minute * 5,
		WriteTimeout: time.Minute * 5,
	})
	return &Redis{client}
}

// Close close redis store
func (s Redis) Close() error {
	if s.client == nil {
		return nil
	}

	return s.client.Close()
}

// Exists check quote exists
func (s Redis) Exists(exchange exchanges.Exchange, date time.Time) (bool, error) {
	// key: et:{exchange}:{date} value:1 / 0 (is trading day)
	key := fmt.Sprintf("et:%s:%s", exchange.Code(), date.Format(constants.DatePattern))
	exists, err := s.client.Exists(key).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}

	return exists == 1, err
}

// Save save exchange daily quote
func (s Redis) Save(exchange exchanges.Exchange, date time.Time, edq *quotes.ExchangeDailyQuote) error {

	var pairs []string

	// save exchange daily
	isTrading := "1"
	if edq.IsEmpty() {
		isTrading = "0"
	}

	// key: et:{exchange}:{date} value:1 / 0 (is trading day)
	pairs = append(pairs, fmt.Sprintf("et:%s:%s", exchange.Code(), date.Format(constants.DatePattern)), isTrading)

	// save exchange daily companies
	pairs = append(pairs, s.saveExchangeDailyCompanies(exchange, date, edq.Companies)...)

	// save exchange daily company quotes
	for _, cdq := range edq.Quotes {
		// save company rollup
		// key: 1d:{exchange}:{companyCode}:{date} value:{open},{close},{high},{low},{volume}
		rollup := cdq.Regular.Rollup()
		key := fmt.Sprintf("1d:%s:%s:%s", exchange.Code(), cdq.Company.Code, date.Format(constants.DatePattern))
		pairs = append(pairs, key, s.formatQuote(*rollup))

		// save dividend
		if cdq.Dividend != nil && cdq.Dividend.Enable {
			pairs = append(pairs, s.saveCompanyDividend(exchange, cdq.Company, date, cdq.Dividend)...)
		}

		// save split
		if cdq.Split != nil && cdq.Split.Enable {
			pairs = append(pairs, s.saveCompanySplit(exchange, cdq.Company, date, cdq.Split)...)
		}

		// save pre
		pairs = append(pairs, s.saveCompanyDailyQuoteSerial(exchange, cdq.Company, date, quotes.SerialTypePre, cdq.Pre)...)

		// save regular
		pairs = append(pairs, s.saveCompanyDailyQuoteSerial(exchange, cdq.Company, date, quotes.SerialTypeRegular, cdq.Regular)...)

		// save post
		pairs = append(pairs, s.saveCompanyDailyQuoteSerial(exchange, cdq.Company, date, quotes.SerialTypePost, cdq.Post)...)
	}

	err := s.client.MSet(pairs).Err()
	if err != nil {
		zap.L().Error("save exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date),
			zap.Int("keys", len(pairs)/2))
		return err
	}

	zap.L().Debug("save exchange daily quote success",
		zap.Error(err),
		zap.String("exchange", exchange.Code()),
		zap.Time("date", date),
		zap.Int("keys", len(pairs)/2))

	return nil
}

func (s Redis) saveExchangeDailyCompanies(exchange exchanges.Exchange, date time.Time, companies map[string]*quotes.Company) []string {
	if len(companies) == 0 {
		return []string{}
	}

	// key: ec:{exchange}:{date}:{companyCode} value:{companyName}
	pairs := make([]string, len(companies)*2)
	index := 0
	for _, company := range companies {
		pairs[index*2] = fmt.Sprintf("ec:%s:%s:%s", exchange.Code(), date.Format(constants.DatePattern), company.Code)
		pairs[index*2+1] = company.Name
		index++
	}

	return pairs
}

func (s Redis) saveCompanyDividend(exchange exchanges.Exchange, company *quotes.Company, date time.Time, dividend *quotes.Dividend) []string {
	// key: dividend:{exchange}:{companyCode}:{date} value:{timestamp},{amount}
	key := fmt.Sprintf("dividend:%s:%s:%s", exchange.Code(), company.Code, date.Format(constants.DatePattern))
	value := fmt.Sprintf("%d,%f", dividend.Timestamp, dividend.Amount)
	return []string{key, value}
}

func (s Redis) saveCompanySplit(exchange exchanges.Exchange, company *quotes.Company, date time.Time, split *quotes.Split) []string {
	// key: split:{exchange}:{companyCode}:{date} value:{timestamp},{numerator},{denominator}
	key := fmt.Sprintf("split:%s:%s:%s", exchange.Code(), company.Code, date.Format(constants.DatePattern))
	value := fmt.Sprintf("%d,%f,%f", split.Timestamp, split.Numerator, split.Denominator)
	return []string{key, value}
}

func (s Redis) saveCompanyDailyQuoteSerial(exchange exchanges.Exchange, company *quotes.Company, date time.Time, serialType quotes.SerialType, serial *quotes.Serial) []string {
	if serial == nil || len(*serial) == 0 {
		return []string{}
	}

	dateText := date.Format(constants.DatePattern)
	pairs := make([]string, len(*serial)*2)
	for index, quote := range *serial {
		// key: 1m:{exchange}:{companyCode}:{date}:{Pre|Regular|Post}:{timestamp} value:{open},{close},{high},{low},{volume}
		pairs[index*2] = fmt.Sprintf("1m:%s:%s:%s:%s:%d",
			exchange.Code(),
			company.Code,
			dateText,
			serialType.String(),
			quote.Timestamp)
		pairs[index*2+1] = s.formatQuote(quote)
	}

	return pairs
}

func (s Redis) formatQuote(quote quotes.Quote) string {
	return fmt.Sprintf("%.3f,%.3f,%.3f,%.3f,%d", quote.Open, quote.Close, quote.High, quote.Low, quote.Volume)
}

// Load load exchange daily quote
func (s Redis) Load(exchange exchanges.Exchange, date time.Time) (*quotes.ExchangeDailyQuote, error) {
	// load exchange daily
	// key: et:{exchange}:{date} value:1 / 0 (is trading day)
	key := fmt.Sprintf("et:%s:%s", exchange.Code(), date.Format(constants.DatePattern))
	isTradingDay, err := s.client.Get(key).Result()
	if err != nil {
		if err != redis.Nil {
			zap.L().Error("load exchange daily failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Time("date", date))
			return nil, err
		}
		isTradingDay = "0"
	}

	// is trading day
	if isTradingDay != "1" {
		return &quotes.ExchangeDailyQuote{
			Exchange:  exchange.Code(),
			Date:      date,
			Companies: map[string]*quotes.Company{},
			Quotes:    map[string]*quotes.CompanyDailyQuote{},
		}, nil
	}

	// load exchange daily companies
	companies, err := s.loadExchangeDailyCompanies(exchange, date)
	if err != nil {
		zap.L().Error("load exchange daily companies failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return nil, err
	}

	// load quotes
	cdqs, err := s.loadCompanyQuotes(exchange, date, companies)
	if err != nil {
		zap.L().Error("load exchange daily company quotes failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return nil, err
	}

	edq := &quotes.ExchangeDailyQuote{
		Exchange:  exchange.Code(),
		Date:      date,
		Companies: companies,
		Quotes:    cdqs,
	}

	return edq, nil
}

func (s Redis) loadExchangeDailyCompanies(exchange exchanges.Exchange, date time.Time) (map[string]*quotes.Company, error) {
	// key: ec:{exchange}:{date}:{companyCode} value:{companyName}
	prefix := fmt.Sprintf("ec:%s:%s:", exchange.Code(), date.Format(constants.DatePattern))
	kvs, err := s.prefixScan(prefix)
	if err != nil {
		zap.L().Error("get exchange daily company failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date),
			zap.String("prefix", prefix))
		return nil, err
	}

	companies := make(map[string]*quotes.Company, len(kvs))
	for _, kv := range kvs {
		companies[kv.Key] = &quotes.Company{Code: kv.Key, Name: kv.Value}
	}

	return companies, nil
}

func (s Redis) loadCompanyQuotes(exchange exchanges.Exchange, date time.Time, companies map[string]*quotes.Company) (map[string]*quotes.CompanyDailyQuote, error) {
	cdqs := make(map[string]*quotes.CompanyDailyQuote, len(companies))
	for companyCode, company := range companies {
		// load company rollup
		// key: 1d:{exchange}:{companyCode}:{date} value:{open},{close},{high},{low},{volume}
		key := fmt.Sprintf("1d:%s:%s:%s", exchange.Code(), companyCode, date.Format(constants.DatePattern))
		_, err := s.client.Get(key).Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}

			zap.L().Error("load company rollup failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", company),
				zap.Time("date", date),
				zap.String("key", key))
			return nil, err
		}

		// load dividend
		dividend, err := s.loadCompanyDividend(exchange, date, company)
		if err != nil {
			return nil, err
		}

		// load split
		split, err := s.loadCompanySplit(exchange, date, company)
		if err != nil {
			return nil, err
		}

		// load pre
		pre, err := s.loadCompanyQuoteSerial(exchange, date, company, quotes.SerialTypePre)
		if err != nil {
			return nil, err
		}

		// load regular
		regular, err := s.loadCompanyQuoteSerial(exchange, date, company, quotes.SerialTypeRegular)
		if err != nil {
			return nil, err
		}

		// load post
		post, err := s.loadCompanyQuoteSerial(exchange, date, company, quotes.SerialTypePost)
		if err != nil {
			return nil, err
		}

		cdqs[companyCode] = &quotes.CompanyDailyQuote{
			Company:  company,
			Dividend: dividend,
			Split:    split,
			Pre:      pre,
			Regular:  regular,
			Post:     post,
		}
	}

	return cdqs, nil
}

func (s Redis) loadCompanyDividend(exchange exchanges.Exchange, date time.Time, company *quotes.Company) (*quotes.Dividend, error) {
	// key: dividend:{exchange}:{companyCode}:{date} value:{timestamp},{amount}
	dividend := &quotes.Dividend{Enable: false, Timestamp: 0, Amount: 0}
	key := fmt.Sprintf("dividend:%s:%s:%s", exchange.Code(), company.Code, date.Format(constants.DatePattern))
	value, err := s.client.Get(key).Result()
	if err != nil {
		if err == redis.Nil {
			return dividend, nil
		}

		zap.L().Error("load company dividend failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.String("key", key))
		return nil, err
	}

	_, err = fmt.Sscanf(value, "%d,%f", &dividend.Timestamp, &dividend.Amount)
	if err != nil {
		zap.L().Error("parse company dividend failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.String("key", key),
			zap.String("value", value))
		return nil, err
	}

	dividend.Enable = true
	return dividend, nil
}

func (s Redis) loadCompanySplit(exchange exchanges.Exchange, date time.Time, company *quotes.Company) (*quotes.Split, error) {
	// key: split:{exchange}:{companyCode}:{date} value:{timestamp},{numerator},{denominator}
	split := &quotes.Split{Enable: false, Timestamp: 0, Numerator: 0, Denominator: 0}
	key := fmt.Sprintf("split:%s:%s:%s", exchange.Code(), company.Code, date.Format(constants.DatePattern))
	value, err := s.client.Get(key).Result()
	if err != nil {
		if err == redis.Nil {
			return split, nil
		}

		zap.L().Error("load company split failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.String("key", key))
		return nil, err
	}

	_, err = fmt.Sscanf(value, "%d,%f,%f", &split.Timestamp, &split.Numerator, &split.Denominator)
	if err != nil {
		zap.L().Error("parse company split failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.String("key", key),
			zap.String("value", value))
		return nil, err
	}

	split.Enable = true
	return split, nil
}

func (s Redis) loadCompanyQuoteSerial(exchange exchanges.Exchange, date time.Time, company *quotes.Company, serialType quotes.SerialType) (*quotes.Serial, error) {
	// key: 1m:{exchange}:{companyCode}:{date}:{Pre|Regular|Post}:{timestamp} value:{open},{close},{high},{low},{volume}
	prefix := fmt.Sprintf("1m:%s:%s:%s:%s:", exchange.Code(), company.Code, date.Format(constants.DatePattern), serialType.String())
	kvs, err := s.prefixScan(prefix)
	if err != nil {
		zap.L().Error("get company quote serial failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.String("prefix", prefix))
		return nil, err
	}

	serial := new(quotes.Serial)
	*serial = make([]quotes.Quote, 0, len(kvs))
	for _, kv := range kvs {
		quote, err := s.scanQuote(kv.Key, kv.Value)
		if err != nil {
			zap.L().Error("parse company quote serial failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", company),
				zap.Time("date", date),
				zap.String("serial type", serialType.String()),
				zap.String("key", kv.Value),
				zap.String("value", kv.Value))
			return nil, err
		}

		*serial = append(*serial, *quote)
	}

	return serial, nil
}

func (s Redis) scanQuote(key, value string) (*quotes.Quote, error) {
	// key: 1m:{exchange}:{companyCode}:{date}:{Pre|Regular|Post}:{timestamp} value:{open},{close},{high},{low},{volume}
	parts := strings.Split(key, ":")
	if len(parts) != 6 {
		return nil, fmt.Errorf("invalid company quote serial key: %s", key)
	}

	timestamp, err := strconv.ParseUint(parts[5], 10, 64)
	if err != nil {
		return nil, err
	}

	quote := &quotes.Quote{Timestamp: timestamp}
	_, err = fmt.Sscanf(value, "%f,%f,%f,%f,%d", &quote.Open, &quote.Close, &quote.High, &quote.Low, &quote.Volume)
	if err != nil {
		return nil, err
	}

	return quote, nil
}

// prefixScan perform redis scan with prefix
func (s Redis) prefixScan(prefix string) ([]KV, error) {
	start, end := s.getScanRange(prefix)
	// scan {start} {end} {limit}
	result, err := s.client.Do("scan", start, end, -1).Result()
	if err != nil {
		zap.L().Error("redis scan failed",
			zap.Error(err),
			zap.String("prefix", prefix))
		return nil, err
	}

	slice, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unknown scan result type: %s", reflect.TypeOf(result).String())
	}

	kvs := make([]KV, 0, len(slice)/2)
	for index := 0; index < len(slice); index += 2 {
		kvs = append(kvs, KV{Key: slice[index].(string), Value: slice[index+1].(string)})
	}

	return kvs, nil
}

// getScanRange create start and end pattern with prefix
func (s Redis) getScanRange(prefix string) (string, string) {
	slice := []byte(prefix)
	for index := len(slice) - 1; index >= 0; index-- {
		if slice[index] == 0xff {
			continue
		}

		slice[index]++
		return prefix, string(slice)
	}

	return prefix, prefix
}

// KV key value pair
type KV struct {
	Key   string
	Value string
}
