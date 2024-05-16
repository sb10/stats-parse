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
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

const helpText = `stats-parse parses wrstat stats.gz files quickly, in low mem.

It requires a bom.gids file generated like:

cat /nfs/wrstat/bom.areas | perl -e '%b; while (<>) { chomp; ($g, $b) =
	split(",", $_); $gid = getgrnam($g); push(@{$b{$b}}, $gid); } for $b (sort
	keys %b) { print "$b\t", join(",", @{$b{$b}}), "\n" }' > bom.gids

Specify the path to this file with -b, and also pipe in the uncompressed data
from one or more wrstat stats.gz files.

It will produce tsv output with columns:
* directory
* number of files older than -a years nested within the directory
* size of files (GiB) older than -a years nested within the directory
Where age is determined using the oldest of c and m time. One file per BoM area
will be created, named [-p].[bom area].tsv.

Usage: zcat wrstat.stats.gz | stats-parse -a <int> -b <path>
Options:
  -h          this help text
  -o <string> prefix path to output files
  -a <int>    age of files to report on (years, per oldest of c&mtime)
  -b <string> path to bom.gids file
`

const (
	hoursInDay  = 24
	daysPerYear = 356
)

var l = log.New(os.Stderr, "", 0) //nolint:gochecknoglobals

func main() {
	var (
		help        = flag.Bool("h", false, "print help text")
		prefix      string
		bomGidsFile string
		age         int
	)

	flag.StringVar(&prefix, "o", "output", "prefix path to output files")
	flag.StringVar(&bomGidsFile, "b", "", "path to bom.gids file")
	flag.IntVar(&age, "a", defaultAge, "age of files to report on (years, per oldest of c&mtime)")
	flag.Parse()

	if *help {
		exitHelp("")
	}

	if bomGidsFile == "" {
		exitHelp("ERROR: you must provide the path to bom.gids file")
	}

	if age <= 0 {
		exitHelp("ERROR: -a must be greater than 0")
	}

	gtb := parseBoMGIDsFile(bomGidsFile)
	stats := parseStdin(gtb, age)
	printStats(prefix, stats)
}

// exitHelp prints help text and exits 0, unless a message is passed in which
// case it also prints that and exits 1.
func exitHelp(msg string) {
	print(helpText) //nolint:forbidigo

	if msg != "" {
		fmt.Printf("\n%s\n", msg) //nolint:forbidigo
		os.Exit(1)
	}

	os.Exit(0)
}

func parseBoMGIDsFile(path string) *GIDToBoM {
	bomGIDsFile, err := os.Open(path)
	if err != nil {
		die(err)
	}

	defer bomGIDsFile.Close()

	gtb, err := NewGIDToBoM(bomGIDsFile)
	if err != nil {
		die(err)
	}

	return gtb
}

func parseStdin(gtb *GIDToBoM, age int) []*Stats {
	p := NewStatsParser(os.Stdin)

	stats, err := BoMDirectoryStats(p, gtb, time.Duration(age*daysPerYear*hoursInDay)*time.Hour)
	if err != nil {
		die(err)
	}

	return stats
}

func printStats(prefix string, stats []*Stats) {
	err := PrintBoMDirectoryStats(prefix, stats)
	if err != nil {
		die(err)
	}
}

func die(err error) {
	l.Printf("ERROR: %s", err.Error())
	os.Exit(1)
}
