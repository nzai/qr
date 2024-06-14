package entity

import "github.com/nzai/dbo"

// companies
type Company struct {
	ID     string `gorm:"id" json:"id"`         // id
	Code   string `gorm:"code" json:"code"`     // 编码
	Market string `gorm:"market" json:"market"` // 市场
	Name   string `gorm:"name" json:"name"`     // 名称
}

// TableName table name
func (Company) TableName() string {
	return "companies"
}

type CompanyQueryCondition struct {
	ID      *string   // id
	IDs     *[]string // ids
	Code    *string   // 编码
	Market  *string   // 市场
	Name    *string   // 名称
	OrderBy string
	*dbo.Pager
}

func (c CompanyQueryCondition) GetConditions() ([]string, []any) {
	conditions := make([]string, 0, 4)
	parameters := make([]any, 0, 4)

	if c.ID != nil {
		conditions = append(conditions, "id=?")
		parameters = append(parameters, *c.ID)
	}

	if c.IDs != nil {
		conditions = append(conditions, "id in (?)")
		parameters = append(parameters, *c.IDs)
	}

	if c.Code != nil {
		conditions = append(conditions, "code=?")
		parameters = append(parameters, *c.Code)
	}

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

func (c CompanyQueryCondition) GetOrderBy() string {
	return c.OrderBy
}

func (c CompanyQueryCondition) GetPager() *dbo.Pager {
	return c.Pager
}
