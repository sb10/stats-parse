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

// GIDToBoM is a parser for bom.gids files, which look like:
//
//	bom1\tgid1,gid2
//	bom2\tgid3,gid4,gid5
//
// and can tell you which BoM any particular GID belongs to.
type GIDToBoM struct {
	gidToBom map[int]string
}

// NewGIDToBoM parses the given bom.gids data and returns a GIDTOBoM that can
// tell you the BoM area a GID belongs to.
func NewGIDToBoM(r io.Reader) (*GIDToBoM, error) {
	gidToBom, err := parseBomGIDsData(r)
	if err != nil {
		return nil, err
	}

	return &GIDToBoM{
		gidToBom: gidToBom,
	}, nil
}

func parseBomGIDsData(r io.Reader) (map[int]string, error) {
	gidToBom := make(map[int]string)

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		bom, gids, err := parseBomGIDsLine(line)
		if err != nil {
			return nil, err
		}

		for _, gid := range gids {
			gidToBom[gid] = bom
		}
	}

	if scanner.Err() != nil {
		return nil, scanner.Err()
	}

	return gidToBom, nil
}

func parseBomGIDsLine(line string) (string, []int, error) {
	cols := strings.Split(line, "\t")
	if len(cols) != numBomGIDsColumns {
		return "", nil, Error("invalid bom.gids line: " + line)
	}

	rawBoM, gidsCSV := cols[0], cols[1]

	bom := refomatBoM(rawBoM)
	gids, err := gidsCSVtoGIDs(gidsCSV)

	return bom, gids, err
}

func refomatBoM(rawBoM string) string {
	return strings.ReplaceAll(rawBoM, " ", "")
}

func gidsCSVtoGIDs(gidsCSV string) ([]int, error) {
	gidStrs := strings.Split(gidsCSV, ",")
	gids := make([]int, len(gidStrs))

	for i, gidStr := range gidStrs {
		gid, err := strconv.Atoi(gidStr)
		if err != nil {
			return nil, err
		}

		gids[i] = gid
	}

	return gids, nil
}

// GetBom returns the BoM that the given group belongs to. Returns an error
// if the given GID did not appear in the bom.gids data parsed.
func (p *GIDToBoM) GetBom(gid int) (string, error) {
	bom, ok := p.gidToBom[gid]

	if !ok {
		return "", ErrInvalidGID
	}

	return bom, nil
}
