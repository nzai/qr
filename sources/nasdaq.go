package sources

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nzai/qr/constants"
	"github.com/nzai/qr/quotes"
	"github.com/nzai/qr/utils"
	"go.uber.org/zap"
)

type NasdaqSource struct{}

func NewNasdaqSource() *NasdaqSource {
	return &NasdaqSource{}
}

func (s NasdaqSource) Companies(exchange string) (map[string]*quotes.Company, error) {
	url := fmt.Sprintf("https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&exchange=%s&download=true", strings.ToUpper(exchange))
	headers := map[string]string{
		"Accept":     "*/*",
		"User-Agent": "PostmanRuntime/7.32.2",
	}
	_, content, err := utils.TryDownloadBytesWithHeader(url, headers, constants.RetryCount, constants.RetryInterval)
	if err != nil {
		zap.L().Error("download company symbol list failed", zap.Error(err), zap.String("url", url))
		return nil, err
	}

	data := &NasdayStock{}
	err = json.Unmarshal(content, data)
	if err != nil {
		zap.L().Error("unmarshal symbol list failed", zap.Error(err), zap.String("url", url))
		return nil, err
	}

	companies := make(map[string]*quotes.Company, len(data.Data.Rows))
	for _, row := range data.Data.Rows {
		companies[row.Symbol] = &quotes.Company{Code: row.Symbol, Name: row.Name}
	}

	return companies, nil
}

type NasdayStock struct {
	Data struct {
		AsOf    any `json:"asOf"`
		Headers struct {
			Symbol    string `json:"symbol"`
			Name      string `json:"name"`
			Lastsale  string `json:"lastsale"`
			Netchange string `json:"netchange"`
			Pctchange string `json:"pctchange"`
			MarketCap string `json:"marketCap"`
			Country   string `json:"country"`
			Ipoyear   string `json:"ipoyear"`
			Volume    string `json:"volume"`
			Sector    string `json:"sector"`
			Industry  string `json:"industry"`
			URL       string `json:"url"`
		} `json:"headers"`
		Rows []*NasdaqStockRow `json:"rows"`
	} `json:"data"`
	Message any `json:"message"`
	Status  struct {
		RCode            int `json:"rCode"`
		BCodeMessage     any `json:"bCodeMessage"`
		DeveloperMessage any `json:"developerMessage"`
	} `json:"status"`
}

type NasdaqStockRow struct {
	Symbol    string `json:"symbol"`
	Name      string `json:"name"`
	Lastsale  string `json:"lastsale"`
	Netchange string `json:"netchange"`
	Pctchange string `json:"pctchange"`
	Volume    string `json:"volume"`
	MarketCap string `json:"marketCap"`
	Country   string `json:"country"`
	Ipoyear   string `json:"ipoyear"`
	Industry  string `json:"industry"`
	Sector    string `json:"sector"`
	URL       string `json:"url"`
}
