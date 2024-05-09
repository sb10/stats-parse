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
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const firstLineOfTestFile = `L2x1c3RyZS9zY3JhdGNoMTIyL3RvbC90ZWFtcy9ibGF4dGVyL3VzZXJzL2FtNzUvYXNzZW1ibGllcy9kYXRhc2V0L2lsWGVzU2V4czEuMl9nZW5vbWljLmZuYQ==	646315412	21967	15078	1699895920	1698792671	1698917473	f	144116446803265182	1	2983906128` //nolint:lll

func TestParser(t *testing.T) {
	Convey("Given a parser and reader", t, func() {
		f, err := os.Open("test.stats.gz")
		So(err, ShouldBeNil)

		defer f.Close()

		gr, err := gzip.NewReader(f)
		So(err, ShouldBeNil)

		defer gr.Close()

		p := New(gr)

		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)

		Convey("And an age in years, you can get extract info for files older than the specified age", func() {
			var i int
			for p.ScanForOldFiles(7) {
				if i == 0 {
					So(p.Path, ShouldEqual, "/lustre/scratch122/tol/teams/blaxter/users/cc51/software/samtools-1.9/htslib-1.9/hfile_net.c") //nolint:lll
					So(p.Size, ShouldEqual, 3192)
					So(p.GID, ShouldEqual, 15078)
					So(p.MTime, ShouldEqual, 1437483022)
					So(p.CTime, ShouldEqual, 1703699980)
				} else if i == 1 {
					So(p.Path, ShouldEqual, "/lustre/scratch122/tol/teams/blaxter/users/cc51/software/bcftools-1.19/test/view.filter.10.out") //nolint:lll
					So(p.Size, ShouldEqual, 3754)
					So(p.MTime, ShouldEqual, 1402590965)
				}

				i++
			}
			So(i, ShouldEqual, 6)
		})
	})
}

func BenchmarkScanAndFileInfo(b *testing.B) {
	tempDir := b.TempDir()
	testStatsFile := filepath.Join(tempDir, "test.stats")

	f, err := os.Open("test.stats.gz")
	if err != nil {
		b.Fatal(err)
	}

	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		b.Fatal(err)
	}

	defer gr.Close()

	outFile, err := os.Create(testStatsFile)
	if err != nil {
		b.Fatal(err)
	}

	_, err = io.Copy(outFile, gr)
	if err != nil {
		b.Fatal(err)
	}

	outFile.Close()

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		b.StopTimer()

		f, err := os.Open(testStatsFile)
		if err != nil {
			b.Fatal(err)
		}

		b.StartTimer()

		p := New(f)

		for p.ScanForOldFiles(7) {
			if p.Size == 0 {
				continue
			}
		}

		if p.scanner.Err() != nil {
			fmt.Printf("\nerr: %s\n", p.scanner.Err())

			break
		}

		f.Close()
	}
}

func BenchmarkRawScanner(b *testing.B) {
	for n := 0; n < b.N; n++ {
		f, err := os.Open("test.stats.gz")
		if err != nil {
			continue
		}

		gr, err := gzip.NewReader(f)
		if err != nil {
			continue
		}

		scanner := bufio.NewScanner(gr)

		for scanner.Scan() {
		}
	}
}

func BenchmarkRawScannerUncompressed(b *testing.B) {
	for n := 0; n < b.N; n++ {
		f, err := os.Open("test.stats")
		if err != nil {
			continue
		}

		scanner := bufio.NewScanner(f)

		for scanner.Scan() {
		}
	}
}
