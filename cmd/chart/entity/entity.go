package entity

type Quotes struct {
	Timestamp []int64   `json:"timestamp"`
	Open      []float64 `json:"open"`
	Close     []float64 `json:"close"`
	High      []float64 `json:"high"`
	Low       []float64 `json:"low"`
	Volume    []float64 `json:"volume"`
}

func (q Quotes) Slice() []*Quote {
	slice := make([]*Quote, 0, len(q.Timestamp))
	for index := range q.Timestamp {
		slice = append(slice, &Quote{
			Timestamp: q.Timestamp[index],
			Open:      q.Open[index],
			Close:     q.Close[index],
			High:      q.High[index],
			Low:       q.Low[index],
			Volume:    q.Volume[index],
		})
	}

	return slice
}

type Quote struct {
	Timestamp int64   `json:"timestamp"`
	Open      float64 `json:"open"`
	Close     float64 `json:"close"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Volume    float64 `json:"volume"`
}
