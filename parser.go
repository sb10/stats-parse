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

	ErrBadPath       = Error("invalid file format: path is not base64 encoded")
	ErrTooFewColumns = Error("invalid file format: too few tab separated columns")
)

type Parser struct {
	scanner          *bufio.Scanner
	pathBuffer       []byte
	filter           func() bool
	epochTimeDesired int64
	lineBytes        []byte
	lineLength       int
	lineIndex        int
	Path             []byte
	Size             int64
	GID              int64
	MTime            int64
	CTime            int64
	EntryType        byte
	error            error
}

func New(r io.Reader) *Parser {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, maxLineLength), maxLineLength)

	return &Parser{
		scanner:    scanner,
		pathBuffer: make([]byte, base64.StdEncoding.DecodedLen(maxBase64EncodedPathLength)),
		filter:     noFilter,
	}
}

func noFilter() bool {
	return true
}

func (p *Parser) Scan() bool {
	keepGoing := p.scanner.Scan()
	if !keepGoing {
		return false
	}

	return p.parseLine()
}

func (p *Parser) parseLine() bool {
	p.lineBytes = p.scanner.Bytes()
	p.lineLength = len(p.lineBytes)

	if p.lineLength <= 1 {
		return true
	}

	p.lineIndex = 0

	encodedPath, ok := p.parseNextColumn()
	if !ok {
		return false
	}

	if !p.parseColumns2to7() {
		return false
	}

	entryTypeCol, ok := p.parseNextColumn()
	if !ok {
		return false
	}

	p.EntryType = entryTypeCol[0]

	if filterResult := p.filter(); !filterResult {
		return p.Scan()
	}

	return p.decodePath(encodedPath)
}

func (p *Parser) parseColumns2to7() bool {
	for _, val := range []*int64{&p.Size, nil, &p.GID, nil, &p.MTime, &p.CTime} {
		if !p.parseNumberColumn(val) {
			return false
		}
	}

	return true
}

func (p *Parser) parseNextColumn() ([]byte, bool) {
	start := p.lineIndex

	for p.lineBytes[p.lineIndex] != '\t' {
		p.lineIndex++

		if p.lineIndex >= p.lineLength {
			p.error = ErrTooFewColumns

			return nil, false
		}
	}

	end := p.lineIndex
	p.lineIndex++

	return p.lineBytes[start:end], true
}

func (p *Parser) parseNumberColumn(v *int64) bool {
	col, ok := p.parseNextColumn()
	if !ok {
		return false
	}

	if v == nil {
		return true
	}

	*v = 0

	for _, c := range col {
		*v = *v*10 + int64(c) - '0'
	}

	return true
}

func (p *Parser) decodePath(encodedPath []byte) bool {
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
	p.epochTimeDesired = time.Now().Add(-d).Unix()
}

func (p *Parser) filterForOldFiles() bool {
	if p.EntryType != fileType {
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
