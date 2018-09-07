package stores

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/quotes"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"go.uber.org/zap"
)

// exchange daily				key: {exchange}:{date}												value:1 / 0 (is trading day)
// exchange daily companies		key: {exchange}:{date}:{companyCode}								value:{companyName}
// company daily rollup quote	key: {exchange}:{companyCode}:{date} 								value:{open},{close},{high},{low},{volume}
// company daily quote serial	key: {exchange}:{companyCode}:{date}:{Pre|Regular|Post}:{timestamp}	value:{open},{close},{high},{low},{volume}
// company daily dividend		key: {exchange}:{companyCode}:dividend:{date}						value:{timestamp},{amount}
// company daily split			key: {exchange}:{companyCode}:split:{date}							value:{timestamp},{numerator},{denominator}

// LevelDB level db store
type LevelDB struct {
	db *leveldb.DB
}

// NewLevelDB create level db store
func NewLevelDB(root string) *LevelDB {
	db, err := leveldb.OpenFile(root, nil)
	if err != nil {
		zap.L().Fatal("open db failed", zap.Error(err), zap.String("root", root))
	}
	return &LevelDB{db}
}

// Close close level db store
func (s LevelDB) Close() error {
	if s.db == nil {
		return nil
	}

	return s.db.Close()
}

// Exists check quote exists
func (s LevelDB) Exists(exchange exchanges.Exchange, date time.Time) (bool, error) {
	key := []byte(fmt.Sprintf("%s:%s", exchange.Code(), date.Format(constants.DatePattern)))
	return s.db.Has(key, nil)
}

// Save save exchange daily quote
func (s LevelDB) Save(exchange exchanges.Exchange, date time.Time, edq *quotes.ExchangeDailyQuote) error {
	// create quote save batch
	batch := s.createSaveBatch(exchange, date, edq)
	zap.L().Debug("create save batch success",
		zap.String("exchange", exchange.Code()),
		zap.Time("date", date),
		zap.Int("len", batch.Len()))

	trans, err := s.db.OpenTransaction()
	if err != nil {
		zap.L().Error("open db transaction failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}
	defer trans.Discard()

	err = trans.Write(batch, nil)
	if err != nil {
		zap.L().Error("batch save failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	zap.L().Debug("batch save success",
		zap.String("exchange", exchange.Code()),
		zap.Time("date", date))

	// validate
	saved, err := s.load(trans, exchange, date)
	if err != nil {
		zap.L().Error("load saved exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	zap.L().Debug("load saved exchange daily quote success",
		zap.String("exchange", exchange.Code()),
		zap.Time("date", date))

	err = edq.Equal(*saved)
	if err != nil {
		zap.L().Error("validate saved exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	zap.L().Debug("validate saved exchange daily quote success",
		zap.String("exchange", exchange.Code()),
		zap.Time("date", date))

	err = trans.Commit()
	if err != nil {
		zap.L().Error("commit db transaction failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	return nil
}

func (s LevelDB) createSaveBatch(exchange exchanges.Exchange, date time.Time, edq *quotes.ExchangeDailyQuote) *leveldb.Batch {
	batch := new(leveldb.Batch)

	// save exchange daily
	// key: {exchange}:{date} value:1 / 0 (is trading day)
	if edq.IsEmpty() {
		batch.Put([]byte(fmt.Sprintf("%s:%s", exchange.Code(), date.Format(constants.DatePattern))), []byte{0})
		return batch
	}
	batch.Put([]byte(fmt.Sprintf("%s:%s", exchange.Code(), date.Format(constants.DatePattern))), []byte{1})

	// save exchange daily companies
	// key: {exchange}:{date}:{companyCode} value:{companyName}
	for _, company := range edq.Companies {
		batch.Put([]byte(fmt.Sprintf("%s:%s:%s", exchange.Code(), date.Format(constants.DatePattern), company.Code)), []byte(company.Name))
	}

	// save exchange daily company quotes
	for _, cdq := range edq.Quotes {
		// save company rollup
		// key: {exchange}:{companyCode}:{date} value:{open},{close},{high},{low},{volume}
		rollup := cdq.Regular.Rollup()
		batch.Put([]byte(fmt.Sprintf("%s:%s:%s", exchange.Code(), cdq.Company.Code, date.Format(constants.DatePattern))),
			s.createQuoteBuffer(*rollup))

		// save dividend
		// key: {exchange}:{companyCode}:dividend:{date} value:{timestamp},{amount}
		if cdq.Dividend != nil && cdq.Dividend.Enable {
			batch.Put([]byte(fmt.Sprintf("%s:%s:dividend:%s", exchange.Code(), cdq.Company.Code, date.Format(constants.DatePattern))),
				[]byte(fmt.Sprintf("%d,%f", cdq.Dividend.Timestamp, cdq.Dividend.Amount)))
		}

		// save split
		// key: {exchange}:{companyCode}:split:{date} value:{timestamp},{numerator},{denominator}
		if cdq.Split != nil && cdq.Split.Enable {
			batch.Put([]byte(fmt.Sprintf("%s:%s:split:%s", exchange.Code(), cdq.Company.Code, date.Format(constants.DatePattern))),
				[]byte(fmt.Sprintf("%d,%f,%f", cdq.Split.Timestamp, cdq.Split.Numerator, cdq.Split.Denominator)))
		}

		// save pre
		// key: {exchange}:{companyCode}:{date}:Pre:{timestamp}	value:{open},{close},{high},{low},{volume}
		s.saveCompanyDailyQuoteSerial(batch, exchange, cdq.Company, date, quotes.SerialTypePre, cdq.Pre)

		// save regular
		// key: {exchange}:{companyCode}:{date}:Pre:{timestamp}	value:{open},{close},{high},{low},{volume}
		s.saveCompanyDailyQuoteSerial(batch, exchange, cdq.Company, date, quotes.SerialTypeRegular, cdq.Regular)

		// save post
		// key: {exchange}:{companyCode}:{date}:Pre:{timestamp}	value:{open},{close},{high},{low},{volume}
		s.saveCompanyDailyQuoteSerial(batch, exchange, cdq.Company, date, quotes.SerialTypePost, cdq.Post)
	}

	return batch
}

func (s LevelDB) saveCompanyDailyQuoteSerial(batch *leveldb.Batch, exchange exchanges.Exchange, company *quotes.Company, date time.Time, serialType quotes.SerialType, serial *quotes.Serial) {
	if serial == nil {
		return
	}

	for _, quote := range *serial {
		// key: {exchange}:{companyCode}:{date}:Pre:{timestamp}	value:{open},{close},{high},{low},{volume}
		batch.Put([]byte(fmt.Sprintf("%s:%s:%s:%s:%d",
			exchange.Code(),
			company.Code,
			date.Format(constants.DatePattern),
			serialType.String(),
			quote.Timestamp)),
			s.createQuoteBuffer(quote))
	}
}

func (s LevelDB) createQuoteBuffer(quote quotes.Quote) []byte {
	return []byte(fmt.Sprintf("%f,%f,%f,%f,%d", quote.Open, quote.Close, quote.High, quote.Low, quote.Volume))
}

// Load load exchange daily quote
func (s LevelDB) Load(exchange exchanges.Exchange, date time.Time) (*quotes.ExchangeDailyQuote, error) {
	return s.load(s.db, exchange, date)
}

func (s LevelDB) load(reader leveldb.Reader, exchange exchanges.Exchange, date time.Time) (*quotes.ExchangeDailyQuote, error) {
	// load exchange daily
	// key: {exchange}:{date} value:1 / 0 (is trading day)
	isTradingDay, err := reader.Get([]byte(fmt.Sprintf("%s:%s", exchange.Code(), date.Format(constants.DatePattern))), nil)
	if err != nil {
		zap.L().Error("load exchange daily failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return nil, err
	}

	// is trading day
	if bytes.Equal(isTradingDay, []byte{0}) {
		return &quotes.ExchangeDailyQuote{
			Exchange:  exchange.Code(),
			Date:      date,
			Companies: map[string]*quotes.Company{},
			Quotes:    map[string]*quotes.CompanyDailyQuote{},
		}, nil
	}

	// load exchange daily companies
	companies, err := s.loadExchangeDailyCompanies(reader, exchange, date)
	if err != nil {
		zap.L().Error("load exchange daily companies failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return nil, err
	}

	// load quotes
	cdqs, err := s.loadCompanyQuotes(reader, exchange, date, companies)
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

func (s LevelDB) loadExchangeDailyCompanies(reader leveldb.Reader, exchange exchanges.Exchange, date time.Time) (map[string]*quotes.Company, error) {
	// key: {exchange}:{date}:{companyCode} value:{companyName}
	iter := reader.NewIterator(util.BytesPrefix([]byte(fmt.Sprintf("%s:%s:", exchange.Code(), date.Format(constants.DatePattern)))), nil)
	companies := make(map[string]*quotes.Company)
	for iter.Next() {
		key := string(iter.Key())
		parts := strings.Split(key, ":")
		if len(parts) != 3 {
			zap.L().Error("parse exchange daily companies key failed",
				zap.String("exchange", exchange.Code()),
				zap.Time("date", date),
				zap.String("key", key))
			return nil, fmt.Errorf("invalid exchange daily companies key: %s", key)
		}

		companies[parts[2]] = &quotes.Company{Code: parts[2], Name: string(iter.Value())}
	}
	iter.Release()

	return companies, iter.Error()
}

func (s LevelDB) loadCompanyQuotes(reader leveldb.Reader, exchange exchanges.Exchange, date time.Time, companies map[string]*quotes.Company) (map[string]*quotes.CompanyDailyQuote, error) {
	cdqs := make(map[string]*quotes.CompanyDailyQuote, len(companies))
	for companyCode, company := range companies {
		// load company rollup
		// key: {exchange}:{companyCode}:{date} value:{open},{close},{high},{low},{volume}
		_, err := reader.Get([]byte(fmt.Sprintf("%s:%s:%s", exchange.Code(), companyCode, date.Format(constants.DatePattern))), nil)
		if err != nil {
			if err == leveldb.ErrNotFound {
				continue
			}

			zap.L().Error("load company rollup failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", company),
				zap.Time("date", date))
			return nil, err
		}

		// load dividend
		dividend, err := s.loadCompanyDividend(reader, exchange, date, company)
		if err != nil {
			return nil, err
		}

		// load split
		split, err := s.loadCompanySplit(reader, exchange, date, company)
		if err != nil {
			return nil, err
		}

		// load pre
		pre, err := s.loadCompanyQuoteSerial(reader, exchange, date, company, quotes.SerialTypePre)
		if err != nil {
			return nil, err
		}

		// load regular
		regular, err := s.loadCompanyQuoteSerial(reader, exchange, date, company, quotes.SerialTypeRegular)
		if err != nil {
			return nil, err
		}

		// load post
		post, err := s.loadCompanyQuoteSerial(reader, exchange, date, company, quotes.SerialTypePost)
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

func (s LevelDB) loadCompanyDividend(reader leveldb.Reader, exchange exchanges.Exchange, date time.Time, company *quotes.Company) (*quotes.Dividend, error) {
	// key: {exchange}:{companyCode}:dividend:{date} value:{timestamp},{amount}
	dividend := &quotes.Dividend{Enable: false, Timestamp: 0, Amount: 0}
	value, err := reader.Get([]byte(fmt.Sprintf("%s:%s:dividend:%s", exchange.Code(), company.Code, date.Format(constants.DatePattern))), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return dividend, nil
		}

		zap.L().Error("load company dividend failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date))
		return nil, err
	}

	_, err = fmt.Sscanf(string(value), "%d,%f", &dividend.Timestamp, &dividend.Amount)
	if err != nil {
		zap.L().Error("parse company dividend failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.ByteString("value", value))
		return nil, err
	}

	dividend.Enable = true
	return dividend, nil
}

func (s LevelDB) loadCompanySplit(reader leveldb.Reader, exchange exchanges.Exchange, date time.Time, company *quotes.Company) (*quotes.Split, error) {
	// key: {exchange}:{companyCode}:split:{date} value:{timestamp},{numerator},{denominator}
	split := &quotes.Split{Enable: false, Timestamp: 0, Numerator: 0, Denominator: 0}
	value, err := reader.Get([]byte(fmt.Sprintf("%s:%s:split:%s", exchange.Code(), company.Code, date.Format(constants.DatePattern))), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return split, nil
		}

		zap.L().Error("load company split failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date))
		return nil, err
	}

	_, err = fmt.Sscanf(string(value), "%d,%f,%f", &split.Timestamp, &split.Numerator, &split.Denominator)
	if err != nil {
		zap.L().Error("parse company split failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", company),
			zap.Time("date", date),
			zap.ByteString("value", value))
		return nil, err
	}

	split.Enable = true
	return split, nil
}

func (s LevelDB) loadCompanyQuoteSerial(reader leveldb.Reader, exchange exchanges.Exchange, date time.Time, company *quotes.Company, serialType quotes.SerialType) (*quotes.Serial, error) {
	// key: {exchange}:{companyCode}:{date}:Pre:{timestamp}	value:{open},{close},{high},{low},{volume}
	iter := reader.NewIterator(util.BytesPrefix([]byte(fmt.Sprintf("%s:%s:%s:%s:", exchange.Code(), company.Code, date.Format(constants.DatePattern), serialType.String()))), nil)
	serial := new(quotes.Serial)
	*serial = make([]quotes.Quote, 0)
	for iter.Next() {
		quote, err := s.readQuoteBuffer(iter.Key(), iter.Value())
		if err != nil {
			zap.L().Error("parse company quote serial failed",
				zap.String("exchange", exchange.Code()),
				zap.Any("company", company),
				zap.Time("date", date),
				zap.String("serial type", serialType.String()),
				zap.ByteString("key", iter.Key()),
				zap.ByteString("value", iter.Value()))
			return nil, err
		}

		*serial = append(*serial, *quote)
	}
	iter.Release()

	err := iter.Error()
	if err != nil {
		return nil, err
	}

	return serial, nil
}

func (s LevelDB) readQuoteBuffer(key, value []byte) (*quotes.Quote, error) {
	// key: {exchange}:{companyCode}:{date}:Pre:{timestamp}	value:{open},{close},{high},{low},{volume}
	parts := strings.Split(string(key), ":")
	if len(parts) != 5 {
		return nil, fmt.Errorf("invalid company quote serial key: %s", key)
	}

	timestamp, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		return nil, err
	}

	quote := &quotes.Quote{Timestamp: timestamp}
	_, err = fmt.Sscanf(string(value), "%f,%f,%f,%f,%d", &quote.Open, &quote.Close, &quote.High, &quote.Low, &quote.Volume)
	if err != nil {
		return nil, err
	}

	return quote, nil
}
