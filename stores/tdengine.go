package stores

import (
	"fmt"
	"strings"
	"time"

	"database/sql"

	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/quotes"
	_ "github.com/taosdata/driver-go/taosSql"
	"go.uber.org/zap"
)

const (
	tdeDriverName       = "taosSql"
	flagTypeRaw1mPrefix = "raw_1m_done_"
)

// TDEngine tdengine store
// tables:
// nasdaq_raw_1m_done			done flag
// nasdaq_company				companies
// nasdaq_aapl_pre_raw_1m		pre
// nasdaq_aapl_regular_raw_1m	regular
// nasdaq_aapl_post_raw_1m		post
// nasdaq_aapl_dividend			dividend
// nasdaq_aapl_split			split
type TDEngine struct {
	db *sql.DB
}

// NewTDEngine create new tdengine store
func NewTDEngine(address string) (*TDEngine, error) {
	db, err := sql.Open(tdeDriverName, address)
	if err != nil {
		zap.L().Error("connect tdengine failed", zap.Error(err), zap.String("address", address))
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		zap.L().Error("ping tdengine failed", zap.Error(err), zap.String("address", address))
		return nil, err
	}

	tde := &TDEngine{db: db}

	err = tde.ensureTables()
	if err != nil {
		zap.L().Error("ensure tables failed", zap.Error(err))
		return nil, err
	}

	return tde, nil
}

// Close close store
func (s TDEngine) Close() error {
	if s.db != nil {
		s.db.Close()
	}

	return nil
}

func (s TDEngine) ensureTables() error {
	commands := []string{
		"CREATE TABLE IF NOT EXISTS quotes (ts timestamp, open float, close float, high float, low float, volume bigint) TAGS (exchange binary(32), company binary(32), type binary(16))",
		"CREATE TABLE IF NOT EXISTS symbols (ts timestamp, symbol binary(32), name nchar(256)) TAGS (exchange binary(32), type binary(64))",
		"CREATE TABLE IF NOT EXISTS flags (ts timestamp, flag bigint) TAGS (exchange binary(32), type binary(64))",
		"CREATE TABLE IF NOT EXISTS dividends (ts timestamp, amount float) TAGS (exchange binary(32), company binary(32))",
		"CREATE TABLE IF NOT EXISTS splits (ts timestamp, numerator float, denominator float) TAGS (exchange binary(32), company binary(32))",
	}

	for _, command := range commands {
		_, err := s.db.Exec(command)
		if err != nil {
			zap.L().Error("ensure table failed", zap.Error(err), zap.String("command", command))
			return err
		}
	}

	return nil
}

func (s TDEngine) exchangeDoneTableName(exchange exchanges.Exchange) string {
	return fmt.Sprintf("%s_raw_1m_done", strings.ToLower(exchange.Code()))
}

func (s TDEngine) exchangeCompaniesTableName(exchange exchanges.Exchange) string {
	return fmt.Sprintf("%s_company", strings.ToLower(exchange.Code()))
}

func (s TDEngine) companySerialTableName(exchange exchanges.Exchange, company *quotes.Company, serialType quotes.SerialType) string {
	return fmt.Sprintf("%s_%s_%s_raw_1m",
		strings.ToLower(exchange.Code()),
		strings.ToLower(company.Code),
		strings.ToLower(serialType.String()))
}

func (s TDEngine) companyDividendTableName(exchange exchanges.Exchange, company *quotes.Company) string {
	return fmt.Sprintf("%s_%s_dividend", strings.ToLower(exchange.Code()), strings.ToLower(company.Code))
}

func (s TDEngine) companySplitTableName(exchange exchanges.Exchange, company *quotes.Company) string {
	return fmt.Sprintf("%s_%s_split", strings.ToLower(exchange.Code()), strings.ToLower(company.Code))
}

func (s TDEngine) ensureExchangeTables(exchange exchanges.Exchange) error {
	commands := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s USING flags TAGS ("%s", "raw_1m_done");`,
			s.exchangeDoneTableName(exchange),
			strings.ToLower(exchange.Code())),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s USING symbols TAGS ("%s", "company");`,
			s.exchangeCompaniesTableName(exchange),
			strings.ToLower(exchange.Code())),
	}

	for _, command := range commands {
		_, err := s.db.Exec(command)
		if err != nil {
			zap.L().Error("ensure exchange table failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.String("command", command))
			return err
		}
	}

	return nil
}

func (s TDEngine) ensureCompanyTables(exchange exchanges.Exchange, company *quotes.Company) error {
	commands := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s USING quotes TAGS ("%s", "%s", "raw_1m_%s");`,
			s.companySerialTableName(exchange, company, quotes.SerialTypePre),
			strings.ToLower(exchange.Code()),
			strings.ToLower(company.Code),
			strings.ToLower(quotes.SerialTypePre.String())),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s USING quotes TAGS ("%s", "%s", "raw_1m_%s");`,
			s.companySerialTableName(exchange, company, quotes.SerialTypeRegular),
			strings.ToLower(exchange.Code()),
			strings.ToLower(company.Code),
			strings.ToLower(quotes.SerialTypeRegular.String())),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s USING quotes TAGS ("%s", "%s", "raw_1m_%s");`,
			s.companySerialTableName(exchange, company, quotes.SerialTypePost),
			strings.ToLower(exchange.Code()),
			strings.ToLower(company.Code),
			strings.ToLower(quotes.SerialTypePost.String())),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s USING dividends TAGS ("%s", "%s");`,
			s.companyDividendTableName(exchange, company),
			strings.ToLower(exchange.Code()),
			strings.ToLower(company.Code)),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s USING splits TAGS ("%s", "%s");`,
			s.companySplitTableName(exchange, company),
			strings.ToLower(exchange.Code()),
			strings.ToLower(company.Code)),
	}

	for _, command := range commands {
		_, err := s.db.Exec(command)
		if err != nil {
			zap.L().Error("ensure company table failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.String("company", company.Code),
				zap.String("command", command))
			return err
		}
	}

	return nil
}

// Exists check quote exists
func (s TDEngine) Exists(exchange exchanges.Exchange, date time.Time) (bool, error) {
	command := fmt.Sprintf("select flag from %s where ts=%d", s.exchangeDoneTableName(exchange), date.Unix()*1000)
	row := s.db.QueryRow(command)
	var flag int64
	err := row.Scan(&flag)
	if err == sql.ErrNoRows {
		return false, nil
	}

	if err != nil {
		if err.Error() == "Table does not exist" {
			return false, nil
		}

		zap.L().Error("query exists failed",
			zap.Error(err),
			zap.String("command", command),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return false, err
	}

	return flag > 0, nil
}

// Save save exchange daily quote
func (s TDEngine) Save(exchange exchanges.Exchange, date time.Time, edq *quotes.ExchangeDailyQuote) error {
	err := s.ensureExchangeTables(exchange)
	if err != nil {
		return err
	}

	err = s.saveCompanies(exchange, date, edq.Companies)
	if err != nil {
		return err
	}

	err = s.saveQuotes(exchange, date, edq)
	if err != nil {
		return err
	}

	return s.saveExchangeDone(exchange, date)
}

func (s TDEngine) saveCompanies(exchange exchanges.Exchange, date time.Time, companies map[string]*quotes.Company) error {
	if len(companies) == 0 {
		return nil
	}

	sb := new(strings.Builder)
	ts := date.Unix() * 1000
	index := 0
	for _, company := range companies {
		if index%100 == 0 {
			sb.Reset()
			fmt.Fprintf(sb, "insert into %s values", s.exchangeCompaniesTableName(exchange))
		}

		fmt.Fprintf(sb, "(%d, '%s', '%s')", ts, company.Code, company.Name)

		ts++
		index++

		if index%100 == 0 || index == len(companies) {
			_, err := s.db.Exec(sb.String())
			if err != nil {
				zap.L().Error("save companies failed",
					zap.Error(err),
					zap.String("exchange", exchange.Code()),
					zap.Time("date", date),
					zap.String("sql", sb.String()))
			}
		}
	}

	return nil
}

func (s TDEngine) saveQuotes(exchange exchanges.Exchange, date time.Time, edq *quotes.ExchangeDailyQuote) error {
	var err error
	for _, company := range edq.Companies {
		cdq, found := edq.Quotes[company.Code]
		if !found {
			continue
		}

		err = s.saveCompanyQuotes(exchange, company, date, cdq)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s TDEngine) saveCompanyQuotes(exchange exchanges.Exchange, company *quotes.Company, date time.Time, cdq *quotes.CompanyDailyQuote) error {
	err := s.ensureCompanyTables(exchange, company)
	if err != nil {
		return err
	}

	err = s.saveCompanySerial(exchange, company, quotes.SerialTypePre, cdq.Pre)
	if err != nil {
		return err
	}

	err = s.saveCompanySerial(exchange, company, quotes.SerialTypeRegular, cdq.Regular)
	if err != nil {
		return err
	}

	err = s.saveCompanySerial(exchange, company, quotes.SerialTypePost, cdq.Post)
	if err != nil {
		return err
	}

	err = s.saveCompanyDividend(exchange, company, date, cdq.Dividend)
	if err != nil {
		return err
	}

	err = s.saveCompanySplit(exchange, company, date, cdq.Split)
	if err != nil {
		return err
	}

	return nil
}

func (s TDEngine) saveCompanySerial(exchange exchanges.Exchange, company *quotes.Company, serialType quotes.SerialType, serial *quotes.Serial) error {
	if serial == nil || len(*serial) == 0 {
		return nil
	}

	sb := new(strings.Builder)
	fmt.Fprintf(sb, "insert into %s values", s.companySerialTableName(exchange, company, serialType))

	for _, quote := range *serial {
		fmt.Fprintf(sb, "(%d, %f, %f, %f, %f, %d)", quote.Timestamp*1000, quote.Open, quote.Close, quote.High, quote.Low, quote.Volume)
	}

	_, err := s.db.Exec(sb.String())
	if err != nil {
		zap.L().Error("save company serial failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.String("serialType", serialType.String()))
	}

	return nil
}

func (s TDEngine) saveCompanyDividend(exchange exchanges.Exchange, company *quotes.Company, date time.Time, divide *quotes.Dividend) error {
	if divide == nil || !divide.Enable {
		return nil
	}

	command := fmt.Sprintf("insert into %s values(%d, %f)", s.companyDividendTableName(exchange, company), divide.Timestamp*1000, divide.Amount)
	_, err := s.db.Exec(command)
	if err != nil {
		zap.L().Error("save company divide failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.Time("date", date),
			zap.Any("divide", divide))
	}

	return nil
}

func (s TDEngine) saveCompanySplit(exchange exchanges.Exchange, company *quotes.Company, date time.Time, split *quotes.Split) error {
	if split == nil || !split.Enable {
		return nil
	}

	command := fmt.Sprintf("insert into %s values(%d, %f, %f)", s.companySplitTableName(exchange, company), split.Timestamp*1000, split.Numerator, split.Denominator)
	_, err := s.db.Exec(command)
	if err != nil {
		zap.L().Error("save company split failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.Time("date", date),
			zap.Any("split", split))
	}

	return nil
}

func (s TDEngine) saveExchangeDone(exchange exchanges.Exchange, date time.Time) error {
	command := fmt.Sprintf("insert into %s values(%d, %d)", s.exchangeDoneTableName(exchange), date.Unix()*1000, 1)
	_, err := s.db.Exec(command)
	if err != nil {
		zap.L().Error("save company divide failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
	}

	return nil
}

// Load load exchange daily quote
func (s TDEngine) Load(exchange exchanges.Exchange, date time.Time) (*quotes.ExchangeDailyQuote, error) {
	companies, err := s.loadCompanies(exchange, date)
	if err != nil {
		return nil, err
	}

	companyQuotes, err := s.loadQuotes(exchange, date, companies)
	if err != nil {
		return nil, err
	}

	return &quotes.ExchangeDailyQuote{
		Exchange:  exchange.Code(),
		Date:      date,
		Companies: companies,
		Quotes:    companyQuotes,
	}, nil
}

func (s TDEngine) loadCompanies(exchange exchanges.Exchange, date time.Time) (map[string]*quotes.Company, error) {
	command := fmt.Sprintf("select symbol, name from %s where ts>=%d and ts<%d",
		s.exchangeCompaniesTableName(exchange),
		date.Unix()*1000,
		date.Unix()*1000+86400000)
	rows, err := s.db.Query(command)
	if err != nil {
		zap.L().Error("load exchange companies failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return nil, err
	}
	defer rows.Close()

	companies := make(map[string]*quotes.Company)
	var code, name string
	for rows.Next() {
		err = rows.Scan(&code, &name)
		if err != nil {
			zap.L().Error("scan company failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Time("date", date))
			return nil, err
		}

		companies[code] = &quotes.Company{Code: code, Name: name}
	}

	err = rows.Err()
	if err != nil {
		zap.L().Error("scan rows failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return nil, err
	}

	return companies, nil
}

func (s TDEngine) loadQuotes(exchange exchanges.Exchange, date time.Time, companies map[string]*quotes.Company) (map[string]*quotes.CompanyDailyQuote, error) {
	companyQuotes := make(map[string]*quotes.CompanyDailyQuote, len(companies))
	for _, company := range companies {
		cdq, err := s.loadCompanyQuotes(exchange, date, company)
		if err != nil {
			return nil, err
		}

		companyQuotes[company.Code] = cdq
	}

	return companyQuotes, nil
}

func (s TDEngine) loadCompanyQuotes(exchange exchanges.Exchange, date time.Time, company *quotes.Company) (*quotes.CompanyDailyQuote, error) {
	pre, err := s.loadCompanySerial(exchange, date, company, quotes.SerialTypePre)
	if err != nil {
		return nil, err
	}

	regular, err := s.loadCompanySerial(exchange, date, company, quotes.SerialTypeRegular)
	if err != nil {
		return nil, err
	}

	post, err := s.loadCompanySerial(exchange, date, company, quotes.SerialTypePost)
	if err != nil {
		return nil, err
	}

	dividend, err := s.loadCompanyDividend(exchange, date, company)
	if err != nil {
		return nil, err
	}

	split, err := s.loadCompanySplit(exchange, date, company)
	if err != nil {
		return nil, err
	}

	return &quotes.CompanyDailyQuote{
		Company:  company,
		Dividend: dividend,
		Split:    split,
		Pre:      pre,
		Regular:  regular,
		Post:     post,
	}, nil
}

func (s TDEngine) loadCompanySerial(exchange exchanges.Exchange, date time.Time, company *quotes.Company, serialType quotes.SerialType) (*quotes.Serial, error) {
	command := fmt.Sprintf("select ts, open, close, high, low, volume from %s where ts>=%d and ts<%d order by ts",
		s.companySerialTableName(exchange, company, serialType),
		date.Unix()*1000,
		date.Unix()*1000+86400000)
	rows, err := s.db.Query(command)
	if err != nil {
		zap.L().Error("load company serial failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.Time("date", date),
			zap.String("serialType", serialType.String()))
		return nil, err
	}
	defer rows.Close()

	var serial quotes.Serial
	var volume uint64
	var open, close, high, low float32
	var timeString string
	for rows.Next() {
		err = rows.Scan(&timeString, &open, &close, &high, &low, &volume)
		if err != nil {
			zap.L().Error("scan quote failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.String("company", company.Code),
				zap.Time("date", date),
				zap.String("serialType", serialType.String()))
			return nil, err
		}

		// 2015-05-22 22:30:00.000
		t, err := time.ParseInLocation("2006-01-02 15:04:05.999", timeString, time.Local)
		if err != nil {
			zap.L().Error("parse quote time failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.String("company", company.Code),
				zap.Time("date", date),
				zap.String("serialType", serialType.String()),
				zap.String("timeString", timeString))
			return nil, err
		}

		serial = append(serial, quotes.Quote{
			Timestamp: uint64(t.Unix()),
			Open:      open,
			Close:     close,
			High:      high,
			Low:       low,
			Volume:    volume,
		})

	}

	err = rows.Err()
	if err != nil {
		zap.L().Error("scan quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.Time("date", date),
			zap.String("serialType", serialType.String()))
		return nil, err
	}

	return &serial, nil
}

func (s TDEngine) loadCompanyDividend(exchange exchanges.Exchange, date time.Time, company *quotes.Company) (*quotes.Dividend, error) {
	dividend := &quotes.Dividend{Enable: false}

	command := fmt.Sprintf("select ts, amount from %s where ts=%d",
		s.companyDividendTableName(exchange, company),
		date.Unix()*1000)
	row := s.db.QueryRow(command)

	var timeString string
	var amount float32
	err := row.Scan(&timeString, &amount)
	if err == sql.ErrNoRows {
		return dividend, nil
	}

	if err != nil {
		zap.L().Error("scan dividend failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.Time("date", date))
		return nil, err
	}

	t, err := time.ParseInLocation("2006-01-02 15:04:05.999", timeString, time.Local)
	if err != nil {
		zap.L().Error("parse quote time failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.Time("date", date),
			zap.String("timeString", timeString))
		return nil, err
	}

	dividend.Enable = true
	dividend.Timestamp = uint64(t.Unix())
	dividend.Amount = amount

	return dividend, nil
}

func (s TDEngine) loadCompanySplit(exchange exchanges.Exchange, date time.Time, company *quotes.Company) (*quotes.Split, error) {
	split := &quotes.Split{Enable: false}

	command := fmt.Sprintf("select ts, numerator, denominator from %s where ts=%d",
		s.companySplitTableName(exchange, company),
		date.Unix()*1000)
	row := s.db.QueryRow(command)

	var timeString string
	var numerator, denominator float32
	err := row.Scan(&timeString, &numerator, &denominator)
	if err == sql.ErrNoRows {
		return split, nil
	}

	if err != nil {
		zap.L().Error("scan split failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.Time("date", date))
		return nil, err
	}

	t, err := time.ParseInLocation("2006-01-02 15:04:05.999", timeString, time.Local)
	if err != nil {
		zap.L().Error("parse quote time failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.Time("date", date),
			zap.String("timeString", timeString))
		return nil, err
	}

	split.Enable = true
	split.Timestamp = uint64(t.Unix())
	split.Numerator = numerator
	split.Denominator = denominator

	return split, nil
}

// Delete delete exchange daily quote
func (s TDEngine) Delete(exchange exchanges.Exchange, date time.Time) error {
	// tdengine not allow delete
	return nil
}
