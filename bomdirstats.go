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
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const dirSeparator = string(filepath.Separator)

type Stats struct {
	BoM       string
	Directory string
	Count     uint64
	Size      int64 // in bytes
}

type bomDirKey [2]string
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

		leafDir := filepath.Dir(string(sp.Path))
		dirs := strings.Split(leafDir, dirSeparator)

		accumulateDirStats(dirs, sp, bom, bomToDirToStats)
	}

	return bomToDirToStats, nil
}

func accumulateDirStats(dirs []string, sp *StatsParser, bom string, bomToDirToStats bomDirectoryStats) {
	for i := range dirs {
		thisDir := strings.Join(dirs[0:i], dirSeparator)
		thisDir = strings.TrimSuffix(thisDir, dirSeparator)
		key := bomDirKey{bom, thisDir}

		stats, ok := bomToDirToStats[key]
		if !ok {
			stats = &Stats{
				BoM:       bom,
				Directory: thisDir,
			}
			bomToDirToStats[key] = stats
		}

		stats.Count++
		stats.Size += sp.Size
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

		return cmp.Compare(strings.Count(a.Directory, "/"), strings.Count(b.Directory, "/"))
	})

	return results
}
