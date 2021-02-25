package entity

type ChartData struct {
	Quotes []*Quote `json:"quotes,omitempty"`
}
