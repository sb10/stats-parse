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
	"io"
	"strconv"
	"strings"
)

const (
	ErrInvalidGID     = Error("invalid GID: GID does not belong to any BoMs")
	numBomGIDsColumns = 2
)

// BomGIDsParser is a parser for bom.gids files, which look like:
//
//	bom1\tgid1,gid2
//	bom2\tgid3,gid4,gid5
//
// and can tell you which bom any particular GID belongs to.
type BomGIDsParser struct {
	gidToBom map[int]string
}

// NewBomGIDsParser creates a new BomGIDsParser and parses the given bom.gids
// data.
func NewBomGIDsParser(r io.Reader) (*BomGIDsParser, error) {
	gidToBom, err := parseBomGIDsData(r)
	if err != nil {
		return nil, err
	}

	return &BomGIDsParser{
		gidToBom: gidToBom,
	}, nil
}

func parseBomGIDsData(r io.Reader) (map[int]string, error) {
	gidToBom := make(map[int]string)

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		cols := strings.Split(line, "\t")
		if len(cols) != numBomGIDsColumns {
			return nil, Error("invalid bom.gids line: " + line)
		}

		bom, gidsCSV := cols[0], cols[1]
		bom = strings.ReplaceAll(bom, " ", "")
		gids := strings.Split(gidsCSV, ",")

		for _, gidStr := range gids {
			gid, err := strconv.Atoi(gidStr)
			if err != nil {
				return nil, err
			}

			gidToBom[gid] = bom
		}
	}

	if scanner.Err() != nil {
		return nil, scanner.Err()
	}

	return gidToBom, nil
}

// GetBom returns the BoM that the given group belongs to. Returns an error
// if the given GID did not appear in the bom.gids data parsed.
func (p *BomGIDsParser) GetBom(gid int) (string, error) {
	bom, ok := p.gidToBom[gid]

	if !ok {
		return "", ErrInvalidGID
	}

	return bom, nil
}
