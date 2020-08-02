package quotes

// QuoteType quote type
type QuoteType string

const (
	TypeDaily QuoteType = "daily"
)

type CompanyQuote struct {
	Exchange string
	*Company
	Type   QuoteType
	Peroid int64
	*Quote
}
