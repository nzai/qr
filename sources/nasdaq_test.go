package sources

import (
	"testing"
)

func TestNasdaqSource_Companies(t *testing.T) {
	var source NasdaqSource
	exchanges := []string{"Nasdaq", "Nyse", "Amex"}
	for _, exchange := range exchanges {
		t.Run("exchange", func(t *testing.T) {
			got, err := source.Companies(exchange)
			if err != nil {
				t.Errorf("NasdaqSource.Companies() error = %v", err)
				return
			}

			if len(got) == 0 {
				t.Errorf("NasdaqSource.Companies() = %d", len(got))
			}
		})
	}
}
