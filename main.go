// Copyright © 2024 Genome Research Limited
// Author: Sendu Bala <sb10@sanger.ac.uk>.
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
	"compress/gzip"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

const helpText = `stats-parse parses wrstat stats.gz files quickly, in low mem.

It requires a bom.gids file generated like:

cat /nfs/wrstat/bom.areas | perl -e '%b; while (<>) { chomp; ($g, $b) =
	split(",", $_); $gid = getgrnam($g); push(@{$b{$b}}, $gid); } for $b (sort
	keys %b) { print "$b\t", join(",", @{$b{$b}}), "\n" }' > bom.gids

Specify the path to this file with -b, and also supply the paths to one or more
wrstat stats.gz files as positional arguments.

It will produce tsv output with columns:
* directory
* number of files older than -a years nested within the directory
* size of files (GiB) older than -a years nested within the directory
Where age is determined using the oldest of c and m time.

Usage: stats-parse -a <int> -b <path> wrstat.stats.gz
Options:
  -h          this help text
  -a <int>    age of files to report on (years, per oldest of c&mtime)
  -b <string> path to bom.gids file`

const fileType = byte('f')

const secsPerYear = 3600 * 24 * 365

var l = log.New(os.Stderr, "", 0)

func main() {
	// arg handling
	var (
		help        = flag.Bool("h", false, "print help text")
		bomGidsFile string
		age         int64
	)

	flag.StringVar(&bomGidsFile, "b", "", "path to bom.gids file")
	flag.Int64Var(&age, "a", 7, "age of files to report on (years, per oldest of c&mtime)")
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

	if len(flag.Args()) < 1 {
		exitHelp("ERROR: you must supply some stats.gz files")
	}

	gidToBom := parseBomGids(bomGidsFile)
	parseStatsFiles(flag.Args(), gidToBom, age)
}

// exitHelp prints help text and exits 0, unless a message is passed in which
// case it also prints that and exits 1.
func exitHelp(msg string) {
	fmt.Println(helpText)

	if msg != "" {
		fmt.Printf("\n%s\n", msg)
		os.Exit(1)
	}

	os.Exit(0)
}

func die(err error) {
	l.Printf("ERROR: %s", err.Error())
	os.Exit(1)
}

func parseBomGids(bomGidsFile string) map[int]string {
	// my %gid_to_bom; while (<$fh>) { chomp; my ($bom, $gids) = split("\t", $_);
	// $bom =~ s/\s+//g; foreach my $gid (split(",", $gids)) { $gid_to_bom{$gid} = $bom } } close($fh);
	gidToBom := make(map[int]string)

	file, err := os.Open(bomGidsFile)
	if err != nil {
		die(err)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		cols := strings.Split(line, "\t")
		bom, gidsCSV := cols[0], cols[1]
		bom = strings.ReplaceAll(bom, " ", "")
		gids := strings.Split(gidsCSV, ",")

		for _, gidStr := range gids {
			gid, err := strconv.Atoi(gidStr)
			if err != nil {
				die(err)
			}

			gidToBom[gid] = bom
		}
	}

	if scanner.Err() != nil {
		die(scanner.Err())
	}

	return gidToBom
}

type stats struct {
	count uint64
	size  float64
}

type bomToDirToStats map[string]map[string]stats

func parseStatsFiles(paths []string, gidToBom map[int]string, age int64) {
	results := make(bomToDirToStats)

	for _, path := range paths {
		parseStatsFile(path, gidToBom, age, results)

		break
	}
}

func parseStatsFile(path string, gidToBom map[int]string, age int64, results bomToDirToStats) {
	f, err := os.Open(path)
	if err != nil {
		die(err)
	}

	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		die(err)
	}

	defer gr.Close()

	parseStats(gr, gidToBom, age, results)

	displayResults(results)
}

func parseStats(r io.Reader, gidToBom map[int]string, age int64, results bomToDirToStats) {
	// my @cols = split("\t", $_);
	// next if $cols[7] ne "f";
	// my $t = min($cols[5], $cols[6]);
	// next if $t > $years_ago;
	// my $bom = $gid_to_bom{$cols[3]} || next;
	// my $path = decode_base64($cols[0]);
	// my (undef, $dir, undef) = File::Spec->splitpath($path);
	// my @dirs = File::Spec->splitdir($dir);
	// for my $i (0 .. $#dirs) {
	//   my $dir = join("/", @dirs[0..$i]);
	//   $dir =~ s/\/*$//;
	//   $d{$bom}{$dir}[0]++;
	//   $d{$bom}{$dir}[1] += $cols[1] / $gb_convert;
	// }

	yearsAgo := int(time.Now().Unix() - (secsPerYear * age))

	path := make([]byte, base64.StdEncoding.DecodedLen(1024))

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		b := scanner.Bytes()

		i := 0
		for b[i] != '\t' {
			i++
		}

		encodedPath := b[0:i]

		var size int64

		for i++; b[i] != '\t'; i++ {
			size = size*10 + int64(b[i]) - '0'
		}

		i++
		for b[i] != '\t' {
			i++
		}

		var gid int

		for i++; b[i] != '\t'; i++ {
			gid = gid*10 + int(b[i]) - '0'
		}

		i++
		for b[i] != '\t' {
			i++
		}

		var mtime int

		for i++; b[i] != '\t'; i++ {
			mtime = mtime*10 + int(b[i]) - '0'
		}

		var ctime int

		for i++; b[i] != '\t'; i++ {
			ctime = ctime*10 + int(b[i]) - '0'
		}

		i++

		if b[i] != fileType {
			continue
		}

		if min(mtime, ctime) > yearsAgo {
			continue
		}

		bom, found := gidToBom[gid]
		if !found {
			continue
		}

		l, err := base64.StdEncoding.Decode(path, encodedPath)
		if err != nil {
			die(err)
		}

		fmt.Printf("%s, %d, %d, %d, %d, %s\n", string(path[:l]), size, gid, mtime, ctime, bom)

		break
	}

	if scanner.Err() != nil {
		die(scanner.Err())
	}
}

func displayResults(results bomToDirToStats) {
	// while (my ($bom, $dirs) = each %d) {
	//   open(my $fh, ">$bom.tsv");
	//   foreach my $dir (sort { $a =~ tr/\/// <=> $b =~ tr/\/// || $a cmp $b } keys %{$dirs}) {
	//     my $stats = $dirs->{$dir};
	//     my $gb = sprintf("%.2f", $stats->[1]);
	//     print $fh "$dir\t$stats->[0]\t$gb\n"
	//    }

	for bom, dirStats := range results {
		for dir, stats := range dirStats {
			gb := fmt.Sprintf("%.2f", stats.size)
			fmt.Printf("%s\t%s\t%d\t%s\n", bom, dir, stats.count, gb)
		}
	}
}

func sumColumnThree(path string) int64 {
	file, err := os.Open(path)
	if err != nil {
		die(err)
	}
	defer func() {
		errc := file.Close()
		if errc != nil {
			l.Printf("WARNING: failed to close %s", path)
		}
	}()

	var sum int64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		b := scanner.Bytes()
		i := 0
		for b[i] != '\t' {
			i++
		}
		i++
		for b[i] != '\t' {
			i++
		}
		var depth int
		for i++; i < len(b); i++ {
			depth = depth*10 + int(b[i]) - '0'
		}
		sum += int64(depth)
	}
	return sum
}

func calculateRegions(path string, bpPerRegion int64) {
	var input io.Reader
	if path == "-" {
		input = bufio.NewReader(os.Stdin)
	} else {
		var err error
		input, err = os.Open(path)
		if err != nil {
			die(err)
		}
		defer func() {
			errc := input.(*os.File).Close()
			if errc != nil {
				l.Printf("WARNING: failed to close %s", path)
			}
		}()
	}

	var prevSeq string
	var startPos, lastPos, bpInRegion int64

	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		// parse the line
		b := scanner.Bytes()
		i := 0
		for b[i] != '\t' {
			i++
		}
		seq := string(b[0:i])

		var pos int64
		for i++; b[i] != '\t'; i++ {
			pos = pos*10 + int64(b[i]) - '0'
		}

		var depth int
		for i++; i < len(b); i++ {
			depth = depth*10 + int(b[i]) - '0'
		}

		// print regions that end on sequence changes or when over bpPerRegion.
		// region coordinates are 1-based, inclusive of start and end.
		if prevSeq != seq {
			if prevSeq == "" {
				prevSeq = seq
				startPos = pos
			} else {
				fmt.Printf("%s:%d-%d\n", prevSeq, startPos, lastPos)
				startPos = pos
				prevSeq = seq
				bpInRegion = 0
			}
		}

		bpInRegion += int64(depth)
		if bpInRegion > bpPerRegion {
			fmt.Printf("%s:%d-%d\n", seq, startPos, pos)
			startPos = pos + 1
			bpInRegion = 0
		}

		lastPos = pos
	}

	if lastPos > startPos {
		fmt.Printf("%s:%d-%d\n", prevSeq, startPos, lastPos)
	}
}
