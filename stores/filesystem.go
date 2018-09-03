package stores

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/nzai/qr/exchanges"
	"github.com/nzai/qr/quotes"
	"go.uber.org/zap"
)

// FileSystem define file system store
type FileSystem struct {
	root string
}

// NewFileSystem create file system store
func NewFileSystem(root string) *FileSystem {
	return &FileSystem{root: root}
}

// storePath return store path
func (s FileSystem) storePath(exchange exchanges.Exchange, date time.Time) string {
	return filepath.Join(
		s.root,
		date.Format("2006"),
		date.Format("01"),
		date.Format("02"),
		exchange.Code(),
	)
}

// Exists check quote exists
func (s FileSystem) Exists(exchange exchanges.Exchange, date time.Time) (bool, error) {
	_, err := os.Stat(s.storePath(exchange, date))
	return err == nil, nil
}

// Save save exchange daily quote
func (s FileSystem) Save(exchange exchanges.Exchange, date time.Time, edq *quotes.ExchangeDailyQuote) error {
	// ensure store path
	filePath := s.storePath(exchange, date)
	err := s.ensureDir(filepath.Dir(filePath))
	if err != nil {
		zap.L().Error("ensure save path failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date),
			zap.String("path", filePath))
		return err
	}

	tempPath := filePath + ".temp"
	err = s.save(tempPath, edq)
	if err != nil {
		zap.L().Error("save exchange daily quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date),
			zap.String("path", tempPath))
		return err
	}

	// load saved
	saved, err := s.load(tempPath)
	if err != nil {
		zap.L().Error("load exchange daily quote failed",
			zap.Error(err),
			zap.String("path", tempPath))
		return err
	}

	// valid
	err = edq.Equal(*saved)
	if err != nil {
		zap.L().Error("current quote is different from saved",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))

		err1 := os.Remove(tempPath)
		if err1 != nil {
			zap.L().Error("remove invalid quote failed",
				zap.Error(err1),
				zap.String("exchange", exchange.Code()),
				zap.Time("date", date))
		}

		return err
	}

	err = os.Rename(tempPath, filePath)
	if err != nil {
		zap.L().Error("rename temp exchange daily quote failed",
			zap.Error(err),
			zap.String("tempPath", tempPath),
			zap.String("filePath", filePath))
		return err
	}

	return nil
}

// save exchange daily quote to filePath
func (s FileSystem) save(filePath string, edq *quotes.ExchangeDailyQuote) error {

	file, err := os.Create(filePath)
	if err != nil {
		zap.L().Error("load quote failed", zap.Error(err), zap.String("pth", filePath))
		return err
	}
	defer file.Close()

	// init gzip writer
	gw, err := gzip.NewWriterLevel(file, gzip.BestCompression)
	if err != nil {
		zap.L().Error("create gzip writer failed", zap.Error(err), zap.String("pth", filePath))
		return err
	}

	// encode to gzip writer
	err = edq.Encode(gw)
	if err != nil {
		zap.L().Error("encode quote failed", zap.Error(err), zap.String("pth", filePath))
		return err
	}

	gw.Flush()
	gw.Close()

	return nil
}

// Load load exchange daily quote
func (s FileSystem) Load(exchange exchanges.Exchange, date time.Time) (*quotes.ExchangeDailyQuote, error) {
	// open file
	filePath := s.storePath(exchange, date)
	return s.load(filePath)
}

// load quote from path
func (s FileSystem) load(filePath string) (*quotes.ExchangeDailyQuote, error) {
	file, err := os.Open(filePath)
	if err != nil {
		zap.L().Error("load quote failed", zap.Error(err), zap.String("pth", filePath))
		return nil, err
	}
	defer file.Close()

	// init gzip reader
	gr, err := gzip.NewReader(file)
	if err != nil {
		zap.L().Error("create gzip reader failed", zap.Error(err), zap.String("pth", filePath))
		return nil, err
	}
	defer gr.Close()

	// read unzip bytes
	buffer := new(bytes.Buffer)
	_, err = io.Copy(buffer, gr)
	if err != nil {
		zap.L().Error("read gzip failed", zap.Error(err), zap.String("pth", filePath))
		return nil, err
	}

	// decode from bytes
	edq := new(quotes.ExchangeDailyQuote)
	err = edq.Decode(buffer)
	if err != nil {
		zap.L().Error("decode quote failed", zap.Error(err), zap.String("pth", filePath))
		return nil, err
	}

	return edq, nil
}

// Close close store
func (s FileSystem) Close() error {
	return nil
}

// EnsureDir ensure target dir exists
func (s FileSystem) ensureDir(dir string) error {
	_, err := os.Stat(dir)
	if err == nil {
		return nil
	}

	err = s.ensureDir(filepath.Dir(dir))
	if err != nil {
		return err
	}

	return os.Mkdir(dir, 0755)
}
