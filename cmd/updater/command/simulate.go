package command

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/nzai/qr/cmd/updater/trade_system"
	"github.com/nzai/qr/quotes"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

type Simulate struct{}

func (s Simulate) Command() *cli.Command {
	return &cli.Command{
		Name:    "simulate",
		Aliases: []string{"s"},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "tde-address",
				Aliases:  []string{"tde"},
				Value:    "/tcp(darkred:6030)/quote",
				Required: false,
			},
			&cli.StringFlag{
				Name:     "tablename",
				Aliases:  []string{"table"},
				Value:    "aapl_5m",
				Required: false,
			},
			&cli.Float64Flag{
				Name:     "amount",
				Aliases:  []string{"a"},
				Value:    100000,
				Required: false,
			},
		},
		Action: func(c *cli.Context) error {
			tdeAddress := c.String("tde-address")
			tableName := c.String("tablename")
			amount := c.Float64("amount")

			db, err := s.openDB(tdeAddress)
			if err != nil {
				return err
			}
			defer db.Close()

			qs, err := s.loadData(c.Context, db, tableName)
			if err != nil {
				return err
			}

			if len(qs) == 0 {
				zap.L().Warn("not enough data")
				return nil
			}

			start := time.Now()
			var bestResult *simulateResult
			var bestPeroid int
			for index := 2; index <= 6000; index++ {
				result, err := s.simulate(c.Context, amount, qs, trade_system.NewMA(index))
				// result, err := s.Do(c.Context, amount, qs, trade_system.NewLongHold())
				if err != nil {
					return err
				}

				if bestResult == nil {
					bestResult = result
					bestPeroid = index
					continue
				}

				if result.Profit > bestResult.Profit {
					bestResult = result
					bestPeroid = index
					continue
				}
			}

			// zap.L().Info("do finished", zap.Any("result", result))
			fmt.Printf("peroid: %d\nprofile: %.2f\nprofile percent: %.2f%%\nprice increased: %.2f%%\nduration: %s\n",
				bestPeroid,
				bestResult.Profit,
				bestResult.ProfitPercent*100,
				bestResult.PriceIncreasedPercent*100,
				time.Since(start).String())

			return nil
		},
	}
}

func (s Simulate) openDB(address string) (*sql.DB, error) {
	db, err := sql.Open(tdeDriverName, address)
	if err != nil {
		zap.L().Error("connect tdengine failed", zap.Error(err), zap.String("address", address))
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		zap.L().Error("ping tdengine failed", zap.Error(err), zap.String("address", address))
		return nil, err
	}

	// zap.L().Debug("open database successfully", zap.String("address", address))

	return db, nil
}

func (s Simulate) loadData(ctx context.Context, db *sql.DB, tableName string) ([]*quotes.Quote, error) {
	command := fmt.Sprintf("select ts, open, close, high, low, volume from %s order by ts", tableName)
	rows, err := db.Query(command)
	if err != nil {
		zap.L().Error("load company serial failed",
			zap.Error(err),
			zap.String("tableName", tableName))
		return nil, err
	}
	defer rows.Close()

	var qs []*quotes.Quote
	var volume uint64
	var open, close, high, low float32
	var t time.Time
	for rows.Next() {
		err = rows.Scan(&t, &open, &close, &high, &low, &volume)
		if err != nil {
			zap.L().Error("scan quote failed",
				zap.Error(err),
				zap.String("tableName", tableName))
			return nil, err
		}

		qs = append(qs, &quotes.Quote{
			Timestamp: uint64(t.Unix()),
			Open:      open,
			Close:     close,
			High:      high,
			Low:       low,
			Volume:    volume,
		})
	}

	err = rows.Err()
	if err != nil {
		zap.L().Error("scan quote failed",
			zap.Error(err),
			zap.String("tableName", tableName))
		return nil, err
	}

	return qs, nil
}

func (s Simulate) simulate(ctx context.Context, amount float64, qs []*quotes.Quote, system trade_system.TradeSystem) (*simulateResult, error) {
	err := system.Init(ctx)
	if err != nil {
		zap.L().Error("init trade system failed", zap.Error(err))
		return nil, err
	}

	snapShots := make([]*simulateSnapShot, 0, len(qs))
	context := trade_system.NewContext(ctx, amount)
	for index, quote := range qs {
		context.Current = quote
		context.Prev = qs[:index+1]

		err = system.Next(context)
		if err != nil {
			zap.L().Error("simulate failed", zap.Error(err))
			return nil, err
		}

		cast, quantity := context.Holding()
		snapShots = append(snapShots, &simulateSnapShot{
			Balance:         context.Balance(),
			HoldingCast:     cast,
			HoldingQuantity: quantity,
			Worth:           context.Balance() + float64(context.Current.Close)*float64(quantity),
		})
	}

	err = system.Close(ctx)
	if err != nil {
		zap.L().Error("close trade system failed", zap.Error(err))
		return nil, err
	}

	// zap.L().Info("finished", zap.Any("snapshot", snapShots))
	worth := snapShots[len(snapShots)-1].Worth
	return &simulateResult{
		Profit:                worth - amount,
		ProfitPercent:         (worth - amount) / amount,
		PriceIncreasedPercent: float64((qs[len(qs)-1].Close - qs[0].Close) / qs[0].Close),
		SnapShots:             snapShots,
	}, nil
}

type simulateResult struct {
	Profit                float64             `json:"profit"`
	ProfitPercent         float64             `json:"profit_percent"`
	PriceIncreasedPercent float64             `json:"price_inceased_percent"`
	SnapShots             []*simulateSnapShot `json:"-"`
}

type simulateSnapShot struct {
	Balance         float64 `json:"balance"`
	HoldingCast     float64 `json:"holding_cast"`
	HoldingQuantity uint64  `json:"holding_quantity"`
	Worth           float64 `json:"worth"`
}