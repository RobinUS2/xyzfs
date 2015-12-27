package main

// License: Apache v2
// Source: https://gist.github.com/RobinUS2/ec875b5945f86943f152

import (
	"bytes"
	"compress/gzip"
	// "errors"
	"bufio"
	"hash/crc32"
	"io/ioutil"
	"net/http"
	"strings"
)

func readBodyBytes(r *http.Request) ([]byte, error) {
	// Multi-part?
	if len(r.Header["Content-Type"]) > 0 && strings.Contains(r.Header["Content-Type"][0], "multipart/form-data") {
		// Multi-part reader
		pmfe := r.ParseMultipartForm(1024 * 1024 * 1024)
		if pmfe != nil {
			return nil, pmfe
		}
		// Read parts
		for _, v := range r.MultipartForm.File {
			for _, v2 := range v {
				f, fe := v2.Open()
				if fe != nil {
					return nil, fe
				}
				br := bufio.NewReader(f)
				bb, err := ioutil.ReadAll(br)
				if err != nil {
					return nil, err
				}
				h := crc32.Checksum(bb, crcTable)
				log.Infof("Received bytes %d hash %d", len(bb), h)
				return bb, nil
			}
		}
	}

	// Read body
	bodyBytes, readErr := ioutil.ReadAll(r.Body)
	if readErr != nil {
		return nil, readErr
	}
	defer r.Body.Close()

	// GZIP decode
	if len(r.Header["Content-Encoding"]) > 0 && r.Header["Content-Encoding"][0] == "gzip" {
		r, gzErr := gzip.NewReader(ioutil.NopCloser(bytes.NewBuffer(bodyBytes)))
		if gzErr != nil {
			return nil, gzErr
		}
		defer r.Close()

		bb, err2 := ioutil.ReadAll(r)
		if err2 != nil {
			return nil, err2
		}
		return bb, nil
	} else {
		// Not compressed
		return bodyBytes, nil
	}
}
