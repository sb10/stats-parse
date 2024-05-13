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
	"os"
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
  -b <string> path to bom.gids file
`

// var l = log.New(os.Stderr, "", 0)

func main() {
	// arg handling
	var (
		help        = flag.Bool("h", false, "print help text")
		bomGidsFile string
		age         int64
	)

	flag.StringVar(&bomGidsFile, "b", "", "path to bom.gids file")
	flag.Int64Var(&age, "a", defaultAge, "age of files to report on (years, per oldest of c&mtime)")
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

// func die(err error) {
// 	l.Printf("ERROR: %s", err.Error())
// 	os.Exit(1)
// }

// type stats struct {
// 	count uint64
// 	size  float64
// }

// type bomToDirToStats map[string]map[string]stats

// func parseStatsFiles(paths []string, gidToBom map[int]string, age int64) {
// 	parseStats(gr, gidToBom, age, results)
// 	displayResults(results)
// }

// func parseStats(r io.Reader, gidToBom map[int]string, age int64, results bomToDirToStats) {
// 	// my @cols = split("\t", $_);
// 	// next if $cols[7] ne "f";
// 	// my $t = min($cols[5], $cols[6]);
// 	// next if $t > $years_ago;
// 	// my $bom = $gid_to_bom{$cols[3]} || next;
// 	// my $path = decode_base64($cols[0]);
// 	// my (undef, $dir, undef) = File::Spec->splitpath($path);
// 	// my @dirs = File::Spec->splitdir($dir);
// 	// for my $i (0 .. $#dirs) {
// 	//   my $dir = join("/", @dirs[0..$i]);
// 	//   $dir =~ s/\/*$//;
// 	//   $d{$bom}{$dir}[0]++;
// 	//   $d{$bom}{$dir}[1] += $cols[1] / $gb_convert;
// 	// }

// func displayResults(results bomToDirToStats) {
// 	// while (my ($bom, $dirs) = each %d) {
// 	//   open(my $fh, ">$bom.tsv");
// 	//   foreach my $dir (sort { $a =~ tr/\/// <=> $b =~ tr/\/// || $a cmp $b } keys %{$dirs}) {
// 	//     my $stats = $dirs->{$dir};
// 	//     my $gb = sprintf("%.2f", $stats->[1]);
// 	//     print $fh "$dir\t$stats->[0]\t$gb\n"
// 	//    }

// 	for bom, dirStats := range results {
// 		for dir, stats := range dirStats {
// 			gb := fmt.Sprintf("%.2f", stats.size)
// 			fmt.Printf("%s\t%s\t%d\t%s\n", bom, dir, stats.count, gb)
// 		}
// 	}
// }
