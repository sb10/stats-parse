// Copyright © 2024 Genome Research Limited
// Authors:
//  Sendu Bala <sb10@sanger.ac.uk>.
//  Dan Elia <de7@sanger.ac.uk>.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package main

import (
	"bufio"
	"encoding/base64"
	"io"
	"time"
)

type Error string

func (e Error) Error() string { return string(e) }

const (
	fileType                   = byte('f')
	defaultAge                 = 7
	secsPerYear                = 3600 * 24 * 365
	maxLineLength              = 64 * 1024
	maxBase64EncodedPathLength = 1024

	ErrBadPath = Error("invalid file format: path is not base64 encoded")
)

type Parser struct {
	scanner          *bufio.Scanner
	pathBuffer       []byte
	filter           func([]byte, int) bool
	now              time.Time
	epochTimeDesired int64
	Path             []byte
	Size             int64
	GID              int64
	MTime            int64
	CTime            int64
	error            error
}

func New(r io.Reader) *Parser {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, maxLineLength), maxLineLength)

	return &Parser{
		scanner:    scanner,
		pathBuffer: make([]byte, base64.StdEncoding.DecodedLen(maxBase64EncodedPathLength)),
	}
}

func (p *Parser) Scan() bool {
	keepGoing := p.scanner.Scan()
	if !keepGoing {
		return false
	}

	return p.getInfo()
}

func (p *Parser) getInfo() bool {
	b := p.scanner.Bytes()
	lineLength := len(b)

	i := 0

	skipColumn := func() {
		if i >= lineLength {
			return
		}

		for b[i] != '\t' {
			i++

			if i >= lineLength {
				break
			}
		}
	}

	parseNumberColumn := func(v *int64) {
		*v = 0
		for i++; i < lineLength && b[i] != '\t'; i++ {
			*v = *v*10 + int64(b[i]) - '0'
		}
	}

	skipColumn()

	encodedPath := b[0:i]

	for _, val := range []*int64{&p.Size, nil, &p.GID, nil, &p.MTime, &p.CTime} {
		if val != nil {
			parseNumberColumn(val)
		} else {
			i++

			skipColumn()
		}
	}

	i++

	if p.filter != nil {
		if filterResult := p.filter(b, i); !filterResult {
			return p.Scan()
		}
	}

	l, err := base64.StdEncoding.Decode(p.pathBuffer, encodedPath)
	if err != nil {
		p.error = ErrBadPath

		return false
	}

	p.Path = p.pathBuffer[:l]

	return true
}

func (p *Parser) FilterForFilesOlderThan(d time.Duration) {
	p.filter = p.filterForOldFiles
	p.now = time.Now()
	p.epochTimeDesired = p.now.Add(-d).Unix()
}

func (p *Parser) filterForOldFiles(b []byte, i int) bool {
	if b[i] != fileType {
		return false
	}

	if min(p.MTime, p.CTime) > p.epochTimeDesired {
		return false
	}

	return true
}

// Err returns the first non-EOF error that was encountered by the Scanner.
func (p *Parser) Err() error {
	return p.error
}
