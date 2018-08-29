package quotes

import "io"

// Encoder 可编码的
type Encoder interface {
	Encode(w io.Writer) error
}

// Decoder 可解码的
type Decoder interface {
	Decode(r io.Reader) error
}

// EncodeDecoder 可编码解码的
type EncodeDecoder interface {
	Encoder
	Decoder
}
