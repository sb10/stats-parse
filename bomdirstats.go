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
	"cmp"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"
	"unsafe"
)

type Stats struct {
	BoM       []byte
	Directory string
	Count     uint64
	Size      int64 // in bytes
}

type bomDirKey struct {
	bom       string
	directory string
}

func newBomDirKey(bom, dir []byte) bomDirKey {
	return bomDirKey{unsafe.String(&bom[0], len(bom)), unsafe.String(&dir[0], len(dir))}
}

type bomDirectoryStats map[bomDirKey]*Stats

// BoMDirectoryStats uses the given StatsParser and GIDToBoM to find the number
// and size of all files belonging to each BoM area that are older than the
// given duration, and returns a slice of ?.
func BoMDirectoryStats(sp *StatsParser, gp *GIDToBoM, d time.Duration) ([]*Stats, error) {
	sp.FilterForFilesOlderThan(d)

	bomToDirToStats, err := getBoMDirectoryStats(sp, gp)
	if err != nil {
		return nil, err
	}

	return sortBoMDirectoryStats(bomToDirToStats), nil
}

func getBoMDirectoryStats(sp *StatsParser, gp *GIDToBoM) (bomDirectoryStats, error) {
	bomToDirToStats := make(bomDirectoryStats)

	for sp.Scan() {
		bom, err := gp.GetBom(int(sp.GID))
		if err != nil {
			return nil, err
		}

		accumulateDirStats(sp.Path, sp, bom, bomToDirToStats)
	}

	return bomToDirToStats, sp.Err()
}

func accumulateDirStats(fullPath []byte, sp *StatsParser, bom []byte, bomToDirToStats bomDirectoryStats) {
	for i, b := range fullPath {
		if b == '/' {
			thisDir := fullPath[0 : i+1]
			key := newBomDirKey(bom, thisDir)

			stats, ok := bomToDirToStats[key]
			if !ok {
				stats = &Stats{
					BoM:       bom,
					Directory: string(thisDir[0 : len(thisDir)-1]),
				}
				bomToDirToStats[key] = stats
			}

			stats.Count++
			stats.Size += sp.Size
		}
	}
}

func sortBoMDirectoryStats(bds bomDirectoryStats) []*Stats {
	results := make([]*Stats, 0, len(bds))

	for _, stats := range bds {
		results = append(results, stats)
	}

	slices.SortFunc(results, func(a, b *Stats) int {
		if n := cmp.Compare(b.Size, a.Size); n != 0 {
			return n
		}

		if n := cmp.Compare(strings.Count(a.Directory, "/"), strings.Count(b.Directory, "/")); n != 0 {
			return n
		}

		return cmp.Compare(a.Directory, b.Directory)
	})

	return results
}

// PrintBoMDirectoryStats takes BoMDirectoryStats() stats and writes them as
// a TSV:
//
//	Directory	Count	Size
//
// With one line per Stats and one file per BoM area, with files named after
// the given path suffixed with ".[bom name].tsv".
func PrintBoMDirectoryStats(path string, stats []*Stats) error {
	writers := make(map[string]*os.File)

	for _, s := range stats {
		file, ok := writers[string(s.BoM)]
		if !ok {
			var err error

			file, err = os.Create(fmt.Sprintf("%s.%s.tsv", path, s.BoM))
			if err != nil {
				return err
			}

			defer file.Close()

			writers[string(s.BoM)] = file
		}

		if _, err := fmt.Fprintf(file, "%s\t%d\t%d\n",
			s.Directory, s.Count, s.Size); err != nil {
			return err
		}
	}

	return nil
}
