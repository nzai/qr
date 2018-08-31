package stores

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
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

// Save save quote to dest path
func (s FileSystem) Save(exchange exchanges.Exchange, date time.Time, encoder quotes.Encoder) error {
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

	// init gzip writer
	buffer := new(bytes.Buffer)
	gw, err := gzip.NewWriterLevel(buffer, gzip.BestCompression)
	if err != nil {
		zap.L().Error("create gzip writer failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	// encode to gzip writer
	err = encoder.Encode(gw)
	if err != nil {
		zap.L().Error("encode quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	gw.Flush()
	gw.Close()

	// read zipped bytes
	zipped, err := ioutil.ReadAll(buffer)
	if err != nil {
		zap.L().Error("read zippped bytes failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	// write bytes
	err = ioutil.WriteFile(filePath, zipped, 0660)
	if err != nil {
		zap.L().Error("save quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	return nil
}

// Load load quote from path
func (s FileSystem) Load(exchange exchanges.Exchange, date time.Time, decoder quotes.Decoder) error {
	// open file
	filePath := s.storePath(exchange, date)
	file, err := os.Open(filePath)
	if err != nil {
		zap.L().Error("load quote failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}
	defer file.Close()

	// init gzip reader
	gr, err := gzip.NewReader(file)
	if err != nil {
		zap.L().Error("create gzip reader failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}
	defer gr.Close()

	// read unzip bytes
	buffer := new(bytes.Buffer)
	_, err = io.Copy(buffer, gr)
	if err != nil {
		zap.L().Error("read gzip failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date))
		return err
	}

	// decode from bytes
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

// Remove remove exchange daily quote
func (s FileSystem) Remove(exchange exchanges.Exchange, date time.Time) error {
	exists, _ := s.Exists(exchange, date)
	if !exists {
		return nil
	}

	filePath := s.storePath(exchange, date)
	err := os.Remove(filePath)
	if err != nil {
		zap.L().Error("remove quote file failed",
			zap.Error(err),
			zap.String("exchange", exchange.Code()),
			zap.Time("date", date),
			zap.String("path", filePath))
		return err
	}

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
