package adaptor

import (
	"compress/gzip"
	"io"

	"github.com/vmihailenco/msgpack/v5"
)

type EncoderFactory func(w io.Writer) Encoder

type Encoder interface {
	Encode(v interface{}) error
	Close() error
	Size() int64
	Ext() string
	ContentEncoding() string
}

type msgpackGzipEncoder struct {
	gw      *gzip.Writer
	enc     *msgpack.Encoder
	counter *sizeCounter
}

func (x *msgpackGzipEncoder) Encode(v interface{}) error { return x.enc.Encode(v) }
func (x *msgpackGzipEncoder) Close() error               { return x.gw.Close() }
func (x *msgpackGzipEncoder) Size() int64                { return x.counter.wroteSize }
func (x *msgpackGzipEncoder) Ext() string                { return "msg.gz" }
func (x *msgpackGzipEncoder) ContentEncoding() string    { return "gzip" }

func NewMsgpackEncoder(w io.Writer) Encoder {
	gw := gzip.NewWriter(w)
	counter := &sizeCounter{wr: gw}
	return &msgpackGzipEncoder{
		gw:      gw,
		counter: counter,
		enc:     msgpack.NewEncoder(counter),
	}
}

type sizeCounter struct {
	wr        io.Writer
	wroteSize int64
}

func (x *sizeCounter) Write(p []byte) (int, error) {
	x.wroteSize += int64(len(p))
	return x.wr.Write(p)
}
