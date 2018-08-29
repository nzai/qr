package stores

import (
	"bytes"
	"path/filepath"
	"time"

	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/quotes"
	"go.uber.org/zap"

	"github.com/nzai/go-utility/io"
)

// FileSystem 文件系统存储
type FileSystem struct {
	root string
}

// NewFileSystem 新建文件系统存储
func NewFileSystem(root string) *FileSystem {
	return &FileSystem{root: root}
}

// storePath 存储路径
func (s FileSystem) storePath(exchange exchanges.Exchange, date time.Time) string {
	return filepath.Join(
		s.root,
		date.Format("2006"),
		date.Format("01"),
		date.Format("02"),
		exchange.Code(),
	)
}

// Exists 判断是否存在
func (s FileSystem) Exists(exchange exchanges.Exchange, date time.Time) (bool, error) {
	return io.IsExists(s.storePath(exchange, date)), nil
}

// Save 保存
func (s FileSystem) Save(exchange exchanges.Exchange, date time.Time, encoder quotes.Encoder) error {
	buffer := new(bytes.Buffer)
	err := encoder.Encode(buffer)
	if err != nil {
		zap.L().Error("encode quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	err = io.WriteGzipBytes(s.storePath(exchange, date), buffer.Bytes())
	if err != nil {
		zap.L().Error("save quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	return nil
}

// Load 读取
func (s FileSystem) Load(exchange exchanges.Exchange, date time.Time, decoder quotes.Decoder) error {
	raw, err := io.ReadAllGzipBytes(s.storePath(exchange, date))
	if err != nil {
		zap.L().Error("load quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	buffer := bytes.NewBuffer(raw)
	err = decoder.Decode(buffer)
	if err != nil {
		zap.L().Error("decode quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	return nil
}
