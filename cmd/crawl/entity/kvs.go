package entity

import "github.com/nzai/dbo"

type Kv struct {
	Code  string  `gorm:"code" json:"code"`   // 市场
	Date  string  `gorm:"date" json:"date"`   // 时间YYYYMMDD
	Key   string  `gorm:"key" json:"key"`     // 属性
	Value float64 `gorm:"value" json:"value"` // 值
}

// TableName table name
func (Kv) TableName() string {
	return "kvs"
}

type KvQueryCondition struct {
	Code    *string  // 市场
	Date    *string  // 时间YYYYMMDD
	Key     *string  // 属性
	Value   *float64 // 值
	OrderBy string
	*dbo.Pager
}

func (c KvQueryCondition) GetConditions() ([]string, []any) {
	conditions := make([]string, 0, 5)
	parameters := make([]any, 0, 5)

	if c.Code != nil {
		conditions = append(conditions, "code=?")
		parameters = append(parameters, *c.Code)
	}

	if c.Date != nil {
		conditions = append(conditions, "date=?")
		parameters = append(parameters, *c.Date)
	}

	if c.Key != nil {
		conditions = append(conditions, "key=?")
		parameters = append(parameters, *c.Key)
	}

	if c.Value != nil {
		conditions = append(conditions, "value=?")
		parameters = append(parameters, *c.Value)
	}

	return conditions, parameters
}

func (c KvQueryCondition) GetOrderBy() string {
	return c.OrderBy
}

func (c KvQueryCondition) GetPager() *dbo.Pager {
	return c.Pager
}
