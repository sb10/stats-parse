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
	"slices"
	"strings"
	"time"
)

type Stats struct {
	BoM       []byte
	Directory string
	Count     uint64
	Size      int64 // in bytes
	children  []*Stats
}

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

func getBoMDirectoryStats(sp *StatsParser, gp *GIDToBoM) ([]*Stats, error) {
	bomToStats := make(map[string]*Stats)

	var results []*Stats

	for sp.Scan() {
		bom, err := gp.GetBom(int(sp.GID))
		if err != nil {
			return nil, err
		}

		results = accumulateDirStats(sp.Path, sp, bom, bomToStats, results)
	}

	return results, nil
}

func accumulateDirStats(fullPath []byte, sp *StatsParser, bom []byte, bomToStats map[string]*Stats, results []*Stats) []*Stats {
	stats, ok := bomToStats[string(bom)]
	if !ok {
		stats = &Stats{
			BoM:       bom,
			Directory: "/",
		}
		bomToStats[string(bom)] = stats
		results = append(results, stats)
	}

	stats.Count++
	stats.Size += sp.Size

	for i, b := range fullPath {
		if i == 0 {
			continue
		}

		if b == '/' {
			thisDir := fullPath[0:i]

			newChild := &Stats{
				BoM:       bom,
				Directory: string(thisDir),
			}

			var exists bool

			i, exists := slices.BinarySearchFunc(stats.children, newChild, func(a, b *Stats) int {
				return cmp.Compare(a.Directory, b.Directory)
			})

			if exists {
				stats = stats.children[i]
			} else {
				stats.children = slices.Insert(stats.children, i, newChild)
				stats = newChild
				results = append(results, newChild)
			}

			stats.Count++
			stats.Size += sp.Size
		}
	}

	return results
}

func sortBoMDirectoryStats(results []*Stats) []*Stats {
	slices.SortFunc(results, func(a, b *Stats) int {
		if n := cmp.Compare(b.Size, a.Size); n != 0 {
			return n
		}

		if n := cmp.Compare(strings.Count(a.Directory, "/"), strings.Count(b.Directory, "/")); n != 0 {
			return n
		}

		return cmp.Compare(a.Directory, b.Directory)
	})

	// printStats(results)

	return results
}

func printStats(results []*Stats) {
	fmt.Println("")
	for _, stats := range results {
		fmt.Printf("%s\t%d\t%d\n", stats.Directory, stats.Count, stats.Size)
	}
}
