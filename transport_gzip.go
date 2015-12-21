package main

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
)

type TransportGzip struct {
	_compress   func(in []byte) ([]byte, error)
	_decompress func(in []byte) ([]byte, error)
}

func newTransportGzip() *TransportGzip {
	o := &TransportGzip{}
	o._compress = func(in []byte) ([]byte, error) {
		var b bytes.Buffer
		w := gzip.NewWriter(&b)
		w.Write(in)
		w.Close()
		return b.Bytes(), nil
	}
	o._decompress = func(in []byte) ([]byte, error) {
		br := bytes.NewReader(in)
		r, err := gzip.NewReader(br)
		if err != nil {
			return nil, err
		}
		b, _ := ioutil.ReadAll(r)
		// Ignore errors, can be UEOF
		return b, nil
	}
	return o
}
