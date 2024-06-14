package command

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/nzai/dbo"
	"github.com/nzai/qr/cmd/crawl/entity"
	"github.com/urfave/cli/v3"
	"go.uber.org/zap"
	"gorm.io/gorm/clause"
)

func init() {
	RegisterCommand(&FinancialReport{})
}

type FinancialReport struct {
	mysql string
	date  string
}

func (r *FinancialReport) Command() *cli.Command {
	return &cli.Command{
		Name:    "financial-report",
		Aliases: []string{"fr"},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "mysql",
				Usage:       "specify mysql `connection string`, eg: \"user:password@tcp(127.0.0.1:3306)/quote?parseTime=true&charset=utf8mb4\"",
				Required:    false,
				Value:       "user:password@tcp(127.0.0.1:3306)/quote?parseTime=true&charset=utf8mb4",
				Destination: &r.mysql,
			},
			&cli.StringFlag{
				Name:        "date",
				Usage:       "specify date `2023-03-31`",
				Required:    false,
				Value:       "",
				Destination: &r.date,
				Validator: func(s string) error {
					if s == "" {
						return nil
					}

					if regexp.MustCompile(`^20\d{2}\-(03-31)|(06-30)|(09-30)|(12-31)$`).MatchString(s) {
						return nil
					}

					return fmt.Errorf("invalid date: %s", s)
				},
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			dboHandler, err := dbo.NewWithConfig(func(c *dbo.Config) {
				c.ConnectionString = r.mysql
				c.DBType = dbo.MySQL
				c.LogLevel = dbo.Info
			})
			if err != nil {
				zap.S().Panicw("failed to create dbo", "error", err, "mysql", r.mysql)
			}
			dbo.ReplaceGlobal(dboHandler)

			if r.date != "" {
				zap.S().Infow("try to update", "date", r.date)
				kvs, err := r.getAllPages(ctx, r.date)
				if err != nil {
					return err
				}

				err = r.save(ctx, kvs)
				if err != nil {
					return err
				}

				zap.S().Infow("successfully updated!", "date", r.date, "kvs", len(kvs))
				return nil
			}

			dates := r.getAllDates()
			for _, date := range dates {
				zap.S().Infow("try to update", "date", date)

				kvs, err := r.getAllPages(ctx, date)
				if err != nil {
					return err
				}

				err = r.save(ctx, kvs)
				if err != nil {
					return err
				}

				zap.S().Infow("successfully updated!", "date", date, "kvs", len(kvs))
			}

			return nil
		},
	}
}

func (r FinancialReport) getAllDates() []string {
	var dates []string
	nowYear := time.Now().Year()
	for year := 2013; year <= nowYear; year++ {
		dates = append(dates,
			fmt.Sprintf("%d-03-31", year),
			fmt.Sprintf("%d-06-30", year),
			fmt.Sprintf("%d-09-30", year),
			fmt.Sprintf("%d-12-31", year))
	}

	return dates
}

func (r FinancialReport) getAllPages(ctx context.Context, date string) ([]*entity.Kv, error) {
	kvs := make([]*entity.Kv, 0)
	pageNumber := 1
	var totalPages int
	for {
		datas, total, err := r.getOnePage(ctx, date, pageNumber)
		if err != nil {
			return nil, err
		}

		for _, data := range datas {
			kvs = append(kvs, data.ToKvs()...)
		}

		if pageNumber == 1 {
			totalPages = total
		}

		pageNumber++
		if pageNumber > totalPages {
			break
		}
	}

	zap.S().Debugw("successfully get kvs", "kvs", len(kvs), "pages", totalPages)

	return kvs, nil
}

func (r FinancialReport) save(ctx context.Context, kvs []*entity.Kv) error {
	if len(kvs) == 0 {
		return nil
	}

	err := dbo.MustGetDB(ctx).Clauses(clause.OnConflict{UpdateAll: true}).CreateInBatches(kvs, 1024).Error
	if err != nil {
		zap.S().Errorw("failed to save kvs", "error", err)
		return err
	}

	zap.S().Debugw("successfully save kvs", "kvs", len(kvs))

	return nil
}

func (r FinancialReport) getOnePage(ctx context.Context, date string, pageNumber int) ([]*emFinalcialReportData, int, error) {
	url := fmt.Sprintf("https://datacenter-web.eastmoney.com/api/data/v1/get?pageSize=50&pageNumber=%d&reportName=RPT_LICO_FN_CPD&columns=ALL&filter=(REPORTDATE='%s')", pageNumber, date)
	response, err := httpGet[*emFancialReportResponse](ctx, url, nil)
	if err != nil {
		return nil, 0, err
	}

	return response.Result.Data, response.Result.Pages, nil
}

type emFancialReportResponse struct {
	Version string `json:"version"`
	Result  struct {
		Pages int                      `json:"pages"`
		Data  []*emFinalcialReportData `json:"data"`
		Count int                      `json:"count"`
	} `json:"result"`
	Success bool   `json:"success"`
	Message string `json:"message"`
	Code    int    `json:"code"`
}

type emFinalcialReportData struct {
	SecurityCode       string  `json:"SECURITY_CODE"`
	SecurityNameAbbr   string  `json:"SECURITY_NAME_ABBR"`
	TradeMarketCode    string  `json:"TRADE_MARKET_CODE"`
	TradeMarket        string  `json:"TRADE_MARKET"`
	SecurityTypeCode   string  `json:"SECURITY_TYPE_CODE"`
	SecurityType       string  `json:"SECURITY_TYPE"`
	UpdateDate         string  `json:"UPDATE_DATE"`
	Reportdate         string  `json:"REPORTDATE"`
	BasicEps           float64 `json:"BASIC_EPS"`
	DeductBasicEps     any     `json:"DEDUCT_BASIC_EPS"`
	TotalOperateIncome float64 `json:"TOTAL_OPERATE_INCOME"`
	ParentNetprofit    float64 `json:"PARENT_NETPROFIT"`
	WeightavgRoe       float64 `json:"WEIGHTAVG_ROE"`
	Ystz               float64 `json:"YSTZ"`
	Sjltz              float64 `json:"SJLTZ"`
	Bps                float64 `json:"BPS"`
	Mgjyxjje           float64 `json:"MGJYXJJE"`
	Xsmll              float64 `json:"XSMLL"`
	Yshz               float64 `json:"YSHZ"`
	Sjlhz              float64 `json:"SJLHZ"`
	Assigndscrpt       any     `json:"ASSIGNDSCRPT"`
	Payyear            any     `json:"PAYYEAR"`
	Publishname        string  `json:"PUBLISHNAME"`
	Zxgxl              any     `json:"ZXGXL"`
	NoticeDate         string  `json:"NOTICE_DATE"`
	OrgCode            string  `json:"ORG_CODE"`
	TradeMarketZjg     string  `json:"TRADE_MARKET_ZJG"`
	Isnew              string  `json:"ISNEW"`
	Qdate              string  `json:"QDATE"`
	Datatype           string  `json:"DATATYPE"`
	Datayear           string  `json:"DATAYEAR"`
	Datemmdd           string  `json:"DATEMMDD"`
	Eitime             string  `json:"EITIME"`
	Secucode           string  `json:"SECUCODE"`
}

func (d emFinalcialReportData) ToKvs() []*entity.Kv {
	code := d.SecurityCode
	reportDate, _ := time.Parse("2006-01-02 15:04:05", d.Reportdate)
	date := reportDate.Format("20060102")

	return []*entity.Kv{
		{Code: code, Date: date, Key: "每股收益", Value: d.BasicEps},
		{Code: code, Date: date, Key: "营业总收入", Value: d.TotalOperateIncome},
		{Code: code, Date: date, Key: "营业总收入同比增长", Value: d.Ystz / 100},
		{Code: code, Date: date, Key: "营业总收入季度环比增长", Value: d.Yshz / 100},
		{Code: code, Date: date, Key: "净利润", Value: d.ParentNetprofit},
		{Code: code, Date: date, Key: "净利润同比增长", Value: d.Sjltz / 100},
		{Code: code, Date: date, Key: "净利润季度环比增长", Value: d.Sjlhz / 100},
		{Code: code, Date: date, Key: "每股净资产", Value: d.Bps},
		{Code: code, Date: date, Key: "净资产收益率", Value: d.WeightavgRoe / 100},
		{Code: code, Date: date, Key: "每股经营现金流量", Value: d.Mgjyxjje},
		{Code: code, Date: date, Key: "销售毛利率", Value: d.Xsmll / 100},
	}
}
