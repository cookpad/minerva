package adaptor

import (
	"io"

	"github.com/vmihailenco/msgpack/v5"
)

type DecoderFactory func(w io.ReadCloser) Decoder

type Decoder interface {
	Decode(v interface{}) error
}

type msgpackDecoder struct {
	enc *msgpack.Decoder
}

func NewMsgpackDecoder(r io.ReadCloser) Decoder {
	return msgpack.NewDecoder(r)
}
