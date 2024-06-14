package entity

import "github.com/nzai/dbo"

// markets
type Market struct {
	Market string `gorm:"market" json:"market"` // 编码
	Name   string `gorm:"name" json:"name"`     // 名称
}

// TableName table name
func (Market) TableName() string {
	return "markets"
}

type MarketQueryCondition struct {
	Market  *string // 编码
	Name    *string // 名称
	OrderBy string
	*dbo.Pager
}

func (c MarketQueryCondition) GetConditions() ([]string, []any) {
	conditions := make([]string, 0, 2)
	parameters := make([]any, 0, 2)

	if c.Market != nil {
		conditions = append(conditions, "market=?")
		parameters = append(parameters, *c.Market)
	}

	if c.Name != nil {
		conditions = append(conditions, "name=?")
		parameters = append(parameters, *c.Name)
	}

	return conditions, parameters
}

func (c MarketQueryCondition) GetOrderBy() string {
	return c.OrderBy
}

func (c MarketQueryCondition) GetPager() *dbo.Pager {
	return c.Pager
}
