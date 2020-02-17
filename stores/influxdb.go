package stores

import (
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/influxdata/influxdb1-client" // this is important because of the bug in go mod
	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/quotes"
	"go.uber.org/zap"
)

const (
	companiesMeasurementName   = "companies"
	dividendMeasurementName    = "dividends"
	splitMeasurementName       = "splits"
	minuteQuoteMeasurementName = "q1m"
)

// InfluxDB influxdb store
type InfluxDB struct {
	client client.Client
	db     string
}

// NewInfluxDB create new influxdb store
func NewInfluxDB(address, db string) *InfluxDB {
	c, err := client.NewHTTPClient(client.HTTPConfig{Addr: address})
	if err != nil {
		zap.L().Fatal("create influxdb http client failed", zap.Error(err), zap.String("address", address))
	}

	_, _, err = c.Ping(time.Second)
	if err != nil {
		zap.L().Fatal("ping influxdb failed", zap.Error(err), zap.String("address", address))
	}

	return &InfluxDB{
		client: c,
		db:     db,
	}
}

// Close close store
func (s InfluxDB) Close() error {
	s.client.Close()
	return nil
}

// Exists check quote exists
func (s InfluxDB) Exists(exchange exchanges.Exchange, date time.Time) (bool, error) {
	command := fmt.Sprintf("select count(code) from %s where exchange='%s' and date='%s'",
		companiesMeasurementName,
		exchange.Code(),
		date.Format(constants.DatePattern))

	response, err := s.client.Query(client.NewQuery(command, s.db, ""))
	if err != nil {
		zap.L().Error("query exchange date exists failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return false, err
	}

	err = response.Error()
	if err != nil {
		zap.L().Error("query exchange date exists failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return false, err
	}

	if len(response.Results) == 0 ||
		len(response.Results[0].Series) == 0 ||
		len(response.Results[0].Series[0].Values) == 0 ||
		len(response.Results[0].Series[0].Values[0]) != 2 {
		return false, nil
	}

	count, err := response.Results[0].Series[0].Values[0][1].(json.Number).Int64()
	if err != nil {
		zap.L().Warn("query exchange date exists failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date),
			zap.Any("response", response))
		return false, nil
	}

	return count > 0, nil
}

// Save save exchange daily quote
func (s InfluxDB) Save(exchange exchanges.Exchange, date time.Time, edq *quotes.ExchangeDailyQuote) error {
	err := s.saveCompanies(exchange, date, edq.Companies)
	if err != nil {
		return err
	}

	for _, cdq := range edq.Quotes {
		err = s.saveCompanyDailQuote(exchange, date, cdq)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s InfluxDB) saveCompanies(exchange exchanges.Exchange, date time.Time, companies map[string]*quotes.Company) error {
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Precision: "s",
		Database:  "quotes",
	})

	tags := map[string]string{
		"exchange": exchange.Code(),
		"date":     date.Format(constants.DatePattern),
	}

	t := date
	for _, company := range companies {
		p, _ := client.NewPoint(companiesMeasurementName, tags, map[string]interface{}{
			"code": company.Code,
			"name": company.Name,
		}, t)

		bp.AddPoint(p)
		t = t.Add(time.Second)
	}

	err := s.client.Write(bp)
	if err != nil {
		zap.L().Error("save companies batch points failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	return nil
}

func (s InfluxDB) saveCompanyDailQuote(exchange exchanges.Exchange, date time.Time, cdq *quotes.CompanyDailyQuote) error {
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Precision: "s",
		Database:  "quotes",
	})

	tags := map[string]string{
		"exchange": exchange.Code(),
		"company":  cdq.Company.Code,
	}

	if cdq.Dividend != nil {
		dividend, _ := client.NewPoint(dividendMeasurementName, tags, map[string]interface{}{
			"enable":    cdq.Dividend.Enable,
			"amount":    cdq.Dividend.Amount,
			"timestamp": int64(cdq.Dividend.Timestamp),
		}, date)
		bp.AddPoint(dividend)
	}

	if cdq.Split != nil && cdq.Split.Enable {
		split, _ := client.NewPoint(splitMeasurementName, tags, map[string]interface{}{
			"enable":      cdq.Split.Enable,
			"numerator":   cdq.Split.Numerator,
			"denominator": cdq.Split.Denominator,
			"timestamp":   int64(cdq.Split.Timestamp),
		}, date)
		bp.AddPoint(split)
	}

	bp.AddPoints(s.createQuoteSerialPoints(cdq.Pre, quotes.SerialTypePre, tags))
	bp.AddPoints(s.createQuoteSerialPoints(cdq.Regular, quotes.SerialTypeRegular, tags))
	bp.AddPoints(s.createQuoteSerialPoints(cdq.Post, quotes.SerialTypePost, tags))

	err := s.client.Write(bp)
	if err != nil {
		zap.L().Error("save quotes batch points failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", cdq.Company.Code),
			zap.Time("date", date))
		return err
	}

	return nil
}

func (s InfluxDB) createQuoteSerialPoints(serial *quotes.Serial, st quotes.SerialType, tags map[string]string) []*client.Point {
	tags["serial"] = st.String()
	points := make([]*client.Point, 0, len(*serial))
	for _, quote := range *serial {
		q, _ := client.NewPoint(minuteQuoteMeasurementName, tags, map[string]interface{}{
			"open":   quote.Open,
			"close":  quote.Close,
			"high":   quote.High,
			"low":    quote.Low,
			"volume": int64(quote.Volume),
		}, time.Unix(int64(quote.Timestamp), 0))
		points = append(points, q)
	}

	return points
}

// Load load exchange daily quote
func (s InfluxDB) Load(exchange exchanges.Exchange, date time.Time) (*quotes.ExchangeDailyQuote, error) {
	companies, err := s.loadCompanies(exchange, date)
	if err != nil {
		return nil, err
	}

	cdqs := make(map[string]*quotes.CompanyDailyQuote, len(companies))
	for _, company := range companies {
		cdq, err := s.loadCompanyDailyQuote(exchange, date, company)
		if err != nil {
			return nil, err
		}

		cdqs[cdq.Company.Code] = cdq
	}

	return &quotes.ExchangeDailyQuote{
		Exchange:  exchange.Code(),
		Date:      date,
		Companies: companies,
		Quotes:    cdqs,
	}, nil
}

func (s InfluxDB) loadCompanies(exchange exchanges.Exchange, date time.Time) (map[string]*quotes.Company, error) {
	command := fmt.Sprintf("select code, \"name\" from %s where exchange='%s' and date='%s'",
		companiesMeasurementName,
		exchange.Code(),
		date.Format(constants.DatePattern))

	response, err := s.client.Query(client.NewQuery(command, s.db, ""))
	if err != nil {
		zap.L().Error("query exchange companies failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return nil, err
	}

	err = response.Error()
	if err != nil {
		zap.L().Error("query exchange companies failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return nil, err
	}

	if len(response.Results) == 0 ||
		len(response.Results[0].Series) == 0 ||
		len(response.Results[0].Series[0].Values) == 0 {
		return map[string]*quotes.Company{}, nil
	}

	companies := make(map[string]*quotes.Company, len(response.Results[0].Series[0].Values))
	for _, values := range response.Results[0].Series[0].Values {
		if len(values) != 3 {
			continue
		}

		company := &quotes.Company{
			Code: values[1].(string),
			Name: values[2].(string),
		}

		companies[company.Code] = company
	}

	return companies, nil
}

func (s InfluxDB) loadCompanyDailyQuote(exchange exchanges.Exchange, date time.Time, company *quotes.Company) (*quotes.CompanyDailyQuote, error) {
	dividend, err := s.loadDividend(exchange, date, company)
	if err != nil {
		return nil, err
	}

	split, err := s.loadSplit(exchange, date, company)
	if err != nil {
		return nil, err
	}

	pre, err := s.loadCompanyDailyQuoteSerial(exchange, date, company, quotes.SerialTypePre)
	if err != nil {
		return nil, err
	}

	regular, err := s.loadCompanyDailyQuoteSerial(exchange, date, company, quotes.SerialTypeRegular)
	if err != nil {
		return nil, err
	}

	post, err := s.loadCompanyDailyQuoteSerial(exchange, date, company, quotes.SerialTypePost)
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

func (s InfluxDB) loadDividend(exchange exchanges.Exchange, date time.Time, company *quotes.Company) (*quotes.Dividend, error) {
	command := fmt.Sprintf("select enable, amount, timestamp from %s where exchange='%s' and company='%s' and date='%s'",
		dividendMeasurementName,
		exchange.Code(),
		company.Code,
		date.Format(constants.DatePattern))

	response, err := s.client.Query(client.NewQuery(command, s.db, ""))
	if err != nil {
		zap.L().Error("query company dividend failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.Time("date", date))
		return nil, err
	}

	err = response.Error()
	if err != nil {
		zap.L().Error("query company dividend failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.Time("date", date))
		return nil, err
	}

	if len(response.Results) == 0 ||
		len(response.Results[0].Series) == 0 ||
		len(response.Results[0].Series[0].Values) == 0 ||
		len(response.Results[0].Series[0].Values[0]) != 4 {
		return &quotes.Dividend{}, nil
	}

	enable, ok := response.Results[0].Series[0].Values[0][1].(bool)
	if !ok {
		zap.L().Warn("invalid dividend enable",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.Time("date", date))
		return &quotes.Dividend{}, nil
	}

	amount, err := response.Results[0].Series[0].Values[0][2].(json.Number).Float64()
	if err != nil {
		zap.L().Warn("invalid dividend amount",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.Time("date", date))
		return &quotes.Dividend{}, nil
	}

	timestamp, err := response.Results[0].Series[0].Values[0][3].(json.Number).Int64()
	if err != nil {
		zap.L().Warn("invalid dividend timestamp",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.Time("date", date))
		return &quotes.Dividend{}, nil
	}

	return &quotes.Dividend{
		Enable:    enable,
		Amount:    float32(amount),
		Timestamp: uint64(timestamp),
	}, nil
}

func (s InfluxDB) loadSplit(exchange exchanges.Exchange, date time.Time, company *quotes.Company) (*quotes.Split, error) {
	command := fmt.Sprintf("select enable, numerator, denominator, timestamp from %s where exchange='%s' and company='%s' and date='%s'",
		splitMeasurementName,
		exchange.Code(),
		company.Code,
		date.Format(constants.DatePattern))

	response, err := s.client.Query(client.NewQuery(command, s.db, ""))
	if err != nil {
		zap.L().Error("query company split failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.Time("date", date))
		return nil, err
	}

	err = response.Error()
	if err != nil {
		zap.L().Error("query company split failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.Time("date", date))
		return nil, err
	}

	if len(response.Results) == 0 ||
		len(response.Results[0].Series) == 0 ||
		len(response.Results[0].Series[0].Values) == 0 ||
		len(response.Results[0].Series[0].Values[0]) != 5 {
		return &quotes.Split{}, nil
	}

	enable, ok := response.Results[0].Series[0].Values[0][1].(bool)
	if !ok {
		zap.L().Warn("invalid split enable",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.Time("date", date))
		return &quotes.Split{}, nil
	}

	numerator, err := response.Results[0].Series[0].Values[0][2].(json.Number).Float64()
	if err != nil {
		zap.L().Warn("invalid split numerator",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.Time("date", date))
		return &quotes.Split{}, nil
	}

	denominator, err := response.Results[0].Series[0].Values[0][3].(json.Number).Float64()
	if err != nil {
		zap.L().Warn("invalid split denominator",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.Time("date", date))
		return &quotes.Split{}, nil
	}

	timestamp, err := response.Results[0].Series[0].Values[0][4].(json.Number).Int64()
	if err != nil {
		zap.L().Warn("invalid split timestamp",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.Time("date", date))
		return &quotes.Split{}, nil
	}

	return &quotes.Split{
		Enable:      enable,
		Numerator:   float32(numerator),
		Denominator: float32(denominator),
		Timestamp:   uint64(timestamp),
	}, nil
}

func (s InfluxDB) loadCompanyDailyQuoteSerial(exchange exchanges.Exchange, date time.Time, company *quotes.Company, st quotes.SerialType) (*quotes.Serial, error) {
	command := fmt.Sprintf("select open, close, high, low, volume from %s where exchange='%s' and company='%s' and serial='%s' and time >= '%s' and time < '%s'",
		minuteQuoteMeasurementName,
		exchange.Code(),
		company.Code,
		st.String(),
		date.Format("2006-01-02 15:04:05"),
		date.AddDate(0, 0, 1).Format("2006-01-02 15:04:05"))

	response, err := s.client.Query(client.NewQuery(command, s.db, ""))
	if err != nil {
		zap.L().Error("query company quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.Time("date", date),
			zap.String("serial", st.String()))
		return nil, err
	}

	err = response.Error()
	if err != nil {
		zap.L().Error("query company quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.String("company", company.Code),
			zap.Time("date", date),
			zap.String("serial", st.String()))
		return nil, err
	}

	if len(response.Results) == 0 ||
		len(response.Results[0].Series) == 0 ||
		len(response.Results[0].Series[0].Values) == 0 {
		return &quotes.Serial{}, nil
	}

	serial := make([]quotes.Quote, 0, len(response.Results[0].Series[0].Values))
	for _, values := range response.Results[0].Series[0].Values {
		if len(values) != 6 {
			continue
		}

		t, err := time.Parse(time.RFC3339, values[0].(string))
		if err != nil {
			zap.L().Error("invalid quote timestamp",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.String("company", company.Code),
				zap.Time("date", date),
				zap.String("serial", st.String()),
				zap.Any("timestamp", values[0]))
			return nil, err
		}

		open, err := values[1].(json.Number).Float64()
		if err != nil {
			zap.L().Error("invalid quote open",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.String("company", company.Code),
				zap.Time("date", date),
				zap.String("serial", st.String()),
				zap.Any("open", values[1]))
			return nil, err
		}

		_close, err := values[2].(json.Number).Float64()
		if err != nil {
			zap.L().Error("invalid quote close",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.String("company", company.Code),
				zap.Time("date", date),
				zap.String("serial", st.String()),
				zap.Any("close", values[2]))
			return nil, err
		}

		high, err := values[3].(json.Number).Float64()
		if err != nil {
			zap.L().Error("invalid quote high",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.String("company", company.Code),
				zap.Time("date", date),
				zap.String("serial", st.String()),
				zap.Any("high", values[3]))
			return nil, err
		}

		low, err := values[4].(json.Number).Float64()
		if err != nil {
			zap.L().Error("invalid quote low",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.String("company", company.Code),
				zap.Time("date", date),
				zap.String("serial", st.String()),
				zap.Any("low", values[4]))
			return nil, err
		}

		volume, err := values[5].(json.Number).Int64()
		if err != nil {
			zap.L().Error("invalid quote volume",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.String("company", company.Code),
				zap.Time("date", date),
				zap.String("serial", st.String()),
				zap.Any("volume", values[5]))
			return nil, err
		}

		serial = append(serial, quotes.Quote{
			Timestamp: uint64(t.Unix()),
			Open:      float32(open),
			Close:     float32(_close),
			High:      float32(high),
			Low:       float32(low),
			Volume:    uint64(volume),
		})
	}

	_serial := quotes.Serial(serial)

	return &_serial, nil
}

// Delete delete exchange daily quote
func (s InfluxDB) Delete(exchange exchanges.Exchange, date time.Time) error {
	commands := []string{
		fmt.Sprintf("drop series from %s where exchange='%s' and date='%s'",
			companiesMeasurementName,
			exchange.Code(),
			date.Format(constants.DatePattern)),
		fmt.Sprintf("drop series from %s where exchange='%s' and date='%s'",
			dividendMeasurementName,
			exchange.Code(),
			date.Format(constants.DatePattern)),
		fmt.Sprintf("drop series from %s where exchange='%s' and date='%s'",
			splitMeasurementName,
			exchange.Code(),
			date.Format(constants.DatePattern)),
		fmt.Sprintf("drop series from %s where exchange='%s' and date='%s'",
			minuteQuoteMeasurementName,
			exchange.Code(),
			date.Format(constants.DatePattern)),
	}

	for _, command := range commands {
		response, err := s.client.Query(client.NewQuery(command, s.db, ""))
		if err != nil {
			zap.L().Error("delete exchange daily quote failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Time("date", date),
				zap.String("command", command))
			return err
		}

		err = response.Error()
		if err != nil {
			zap.L().Error("delete exchange daily quote failed",
				zap.Error(err),
				zap.String("exchange", exchange.Code()),
				zap.Time("date", date),
				zap.String("command", command))
			return err
		}
	}

	return nil
}
