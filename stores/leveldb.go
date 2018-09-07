package stores

import (
	"bytes"
	"fmt"
	"time"

	"github.com/nzai/bio"
	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/quotes"
	"github.com/syndtr/goleveldb/leveldb"
	"go.uber.org/zap"
)

// exchange daily				key: {exchange}:{date}												value: 1 / 0 (is trading day)
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

// exchangeDailyKV create exchange daily kv, key is {exchange}:{date}, value is 1(is trading day) or 0
func (s LevelDB) exchangeDailyKV(exchange exchanges.Exchange, date time.Time, isTradingDay bool) ([]byte, []byte) {
	key := []byte(fmt.Sprintf("%s:%s", exchange.Code(), date.Format(constants.DatePattern)))
	value := []byte{byte(0)}
	if isTradingDay {
		value[0] = byte(1)
	}
	return key, value
}

// exchangeDailyCompanyKey create exchange daily company kv, key is {exchange}:{date}:{companyCode}, value is {companyName}
func (s LevelDB) exchangeDailyCompanyKey(exchange exchanges.Exchange, date time.Time, company *quotes.Company) ([]byte, []byte) {
	key := []byte(fmt.Sprintf("%s:%s:%s", exchange.Code(), date.Format(constants.DatePattern), company.Code))
	value := []byte(company.Name)
	return key, value
}

// companyDailyRollupKV create company daily rollup quote, key is {exchange}:{companyCode}:{date}, value is {open},{close},{high},{low},{volume}
func (s LevelDB) companyDailyRollupKV(exchange exchanges.Exchange, date time.Time, company *quotes.Company, quote *quotes.Quote) ([]byte, []byte) {
	key := []byte(fmt.Sprintf("%s:%s:%s", exchange.Code(), company.Code, date.Format(constants.DatePattern)))
	value := []byte(fmt.Sprintf("%f,%f,%f,%f,%d"))
	return key, value
}

// company daily quote serial	key: {exchange}:{companyCode}:{date}:{Pre|Regular|Post}:{timestamp}	value:{open},{close},{high},{low},{volume}
func (s LevelDB) companyDailySerialKV(exchange exchanges.Exchange, company *quotes.Company, date time.Time, serialType quotes.SerialType) ([]byte, []byte) {
	return []byte(fmt.Sprintf("%s:%s:%s:%s", exchange.Code(), company.Code, date.Format(constants.DatePattern), serialType.String()))
}

func (s LevelDB) dividendKey(exchange exchanges.Exchange, company *quotes.Company, date time.Time) ([]byte, []byte) {
	return []byte(fmt.Sprintf("%s:%s:dividend:%s", exchange.Code(), company.Code, date.Format(constants.DatePattern)))
}

func (s LevelDB) splitKey(exchange exchanges.Exchange, company *quotes.Company, date time.Time) ([]byte, []byte) {
	return []byte(fmt.Sprintf("%s:%s:split:%s", exchange.Code(), company.Code, date.Format(constants.DatePattern)))
}

// Exists check quote exists
func (s LevelDB) Exists(exchange exchanges.Exchange, date time.Time) (bool, error) {
	key := []byte(fmt.Sprintf("%s:%s", exchange.Code(), date.Format(constants.DatePattern)))
	return s.db.Has(key, nil)
}

// Save save exchange daily quote
func (s LevelDB) Save(exchange exchanges.Exchange, date time.Time, edq *quotes.ExchangeDailyQuote) error {
	// save quote
	batch, err := s.createSaveBatch(exchange, date, edq)
	if err != nil {
		zap.L().Error("create save batch failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

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

	// validate
	saved, err := s.load(trans, exchange, date)
	if err != nil {
		zap.L().Error("load saved exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	err = edq.Equal(*saved)
	if err != nil {
		zap.L().Error("validate saved exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

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

func (s LevelDB) createSaveBatch(exchange exchanges.Exchange, date time.Time, edq *quotes.ExchangeDailyQuote) (*leveldb.Batch, error) {
	batch := new(leveldb.Batch)

	// save exchange daily

	// save exchange daily companies
	buffer := new(bytes.Buffer)
	bw := bio.NewBinaryWriter(buffer)

	_, err := bw.Int(len(edq.Companies))
	if err != nil {
		zap.L().Error("encode companies count failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date),
			zap.Any("companies", len(edq.Companies)))
		return nil, err
	}

	for _, company := range edq.Companies {
		err = company.Encode(bw)
		if err != nil {
			zap.L().Error("encode company failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Time("date", date),
				zap.Any("companies", len(edq.Companies)))
			return nil, err
		}
	}
	batch.Put(s.exchangeKey(exchange, date), buffer.Bytes())

	// save exchange daily company quotes
	for _, cdq := range edq.Quotes {
		err = s.saveCompanyDailyQuote(batch, exchange, date, cdq)
		if err != nil {
			zap.L().Error("batch save company daily companies failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Time("date", date),
				zap.Any("company", cdq.Company))
			return nil, err
		}
	}

	return batch, nil
}

func (s LevelDB) saveCompanyDailyQuote(batch *leveldb.Batch, exchange exchanges.Exchange, date time.Time, cdq *quotes.CompanyDailyQuote) error {
	// save company rollup
	rollup := cdq.Regular.Rollup()
	err := s.saveAndEncode(batch, s.companyKey(exchange, cdq.Company, date), rollup)
	if err != nil {
		zap.L().Error("batch save company daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Any("company", cdq.Company),
			zap.Time("date", date),
			zap.Any("quote", rollup))
		return err
	}

	// save dividend
	if cdq.Dividend.Enable {
		err = s.saveAndEncode(batch, s.dividendKey(exchange, cdq.Company, date), cdq.Dividend)
		if err != nil {
			zap.L().Error("batch save company dividend failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", cdq.Company),
				zap.Time("date", date),
				zap.Any("dividend", cdq.Dividend))
			return err
		}
	}

	// save split
	if cdq.Split.Enable {
		err = s.saveAndEncode(batch, s.splitKey(exchange, cdq.Company, date), cdq.Split)
		if err != nil {
			zap.L().Error("batch save company split failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", cdq.Company),
				zap.Time("date", date),
				zap.Any("split", cdq.Split))
			return err
		}
	}

	// save pre
	if len(*cdq.Pre) > 0 {
		err = s.saveAndEncode(batch, s.companySerialKey(exchange, cdq.Company, date, quotes.SerialTypePre), cdq.Pre)
		if err != nil {
			zap.L().Error("batch save company daily quote pre serial failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", cdq.Company),
				zap.Time("date", date),
				zap.Int("quote", len(*cdq.Pre)))
			return err
		}
	}

	// save regular
	if len(*cdq.Regular) > 0 {
		err = s.saveAndEncode(batch, s.companySerialKey(exchange, cdq.Company, date, quotes.SerialTypeRegular), cdq.Regular)
		if err != nil {
			zap.L().Error("batch save company daily quote regular serial failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", cdq.Company),
				zap.Time("date", date),
				zap.Int("quote", len(*cdq.Regular)))
			return err
		}
	}

	// save post
	if len(*cdq.Post) > 0 {
		err = s.saveAndEncode(batch, s.companySerialKey(exchange, cdq.Company, date, quotes.SerialTypePost), cdq.Post)
		if err != nil {
			zap.L().Error("batch save company daily quote post serial failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", cdq.Company),
				zap.Time("date", date),
				zap.Int("quote", len(*cdq.Post)))
			return err
		}
	}

	return nil
}

func (s LevelDB) saveAndEncode(batch *leveldb.Batch, key []byte, encoder quotes.Encoder) error {
	buffer := new(bytes.Buffer)
	err := encoder.Encode(buffer)
	if err != nil {
		zap.L().Error("batch save encode failed", zap.Error(err))
		return err
	}

	batch.Put(key, buffer.Bytes())

	return nil
}

// Load load exchange daily quote
func (s LevelDB) Load(exchange exchanges.Exchange, date time.Time) (*quotes.ExchangeDailyQuote, error) {
	return s.load(s.db, exchange, date)
}

func (s LevelDB) load(reader leveldb.Reader, exchange exchanges.Exchange, date time.Time) (*quotes.ExchangeDailyQuote, error) {
	b, err := s.db.Get(s.exchangeKey(exchange, date), nil)
	if err != nil {
		zap.L().Error("load exchange companies failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return nil, err
	}

	buffer := bytes.NewBuffer(b)
	br := bio.NewBinaryReader(buffer)

	// load companies
	count, err := br.Int()
	if err != nil {
		zap.L().Error("decode companies count failed", zap.Error(err))
		return nil, err
	}

	companies := make(map[string]*quotes.Company, count)
	for index := 0; index < count; index++ {
		company := new(quotes.Company)
		err = company.Decode(br)
		if err != nil {
			zap.L().Error("decode company failed", zap.Error(err))
			return nil, err
		}

		companies[company.Code] = company
	}

	// load quotes
	cdqs, err := s.loadCompanyQuotes(reader, exchange, date, companies)
	if err != nil {
		zap.L().Error("load market companies failed",
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

func (s LevelDB) loadCompanyQuotes(reader leveldb.Reader, exchange exchanges.Exchange, date time.Time, companies map[string]*quotes.Company) (map[string]*quotes.CompanyDailyQuote, error) {
	cdqs := make(map[string]*quotes.CompanyDailyQuote, len(companies))
	for companyCode, company := range companies {
		_, err := reader.Get(s.companyKey(exchange, company, date), nil)
		if err == leveldb.ErrNotFound {
			continue
		}

		// load dividend
		dividend := &quotes.Dividend{Enable: false, Timestamp: 0, Amount: 0}
		err = s.loadAndDecode(reader, s.dividendKey(exchange, company, date), dividend, true)
		if err != nil {
			zap.L().Error("load company dividend failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", company),
				zap.Time("date", date))
			return nil, err
		}

		// load split
		split := &quotes.Split{Enable: false, Timestamp: 0, Numerator: 0, Denominator: 0}
		err = s.loadAndDecode(reader, s.splitKey(exchange, company, date), split, true)
		if err != nil {
			zap.L().Error("load company split failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", company),
				zap.Time("date", date))
			return nil, err
		}

		// load pre
		pre := new(quotes.Serial)
		err = s.loadAndDecode(reader, s.companySerialKey(exchange, company, date, quotes.SerialTypePre), pre, true)
		if err != nil {
			zap.L().Error("load company pre serial failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", company),
				zap.Time("date", date))
			return nil, err
		}

		// load regular
		regular := new(quotes.Serial)
		err = s.loadAndDecode(reader, s.companySerialKey(exchange, company, date, quotes.SerialTypeRegular), regular, true)
		if err != nil {
			zap.L().Error("load company regular serial failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", company),
				zap.Time("date", date))
			return nil, err
		}

		// load post
		post := new(quotes.Serial)
		err = s.loadAndDecode(reader, s.companySerialKey(exchange, company, date, quotes.SerialTypePost), post, true)
		if err != nil {
			zap.L().Error("load company post serial failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Any("company", company),
				zap.Time("date", date))
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

func (s LevelDB) loadAndDecode(reader leveldb.Reader, key []byte, decoder quotes.Decoder, ignoreNotFound bool) error {
	value, err := reader.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound && ignoreNotFound {
			return nil
		}

		zap.L().Error("get key failed",
			zap.Error(err),
			zap.ByteString("key", key),
			zap.Bool("ignoreNotFound", ignoreNotFound))
		return err
	}

	buffer := bytes.NewBuffer(value)
	err = decoder.Decode(buffer)
	if err != nil {
		zap.L().Error("decode quote failed",
			zap.Error(err),
			zap.ByteString("key", key))
		return err
	}

	return nil
}
