package quotes

import "io"

// Encoder define types can be encode to io.Writer
type Encoder interface {
	Encode(w io.Writer) error
}

// Decoder define types can be decode from io.Reader
type Decoder interface {
	Decode(r io.Reader) error
}
