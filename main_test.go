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
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

const epochWhenTestFileWasCreated = 1715261665

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

		Convey("you can get extract info for all entries", func() {
			i := 0
			for p.Scan() {
				if i == 0 {
					So(string(p.Path), ShouldEqual, "/lustre/scratch122/tol/teams/blaxter/users/am75/assemblies/dataset/ilXesSexs1.2_genomic.fna") //nolint:lll

				} else if i == 1 {
					So(string(p.Path), ShouldEqual, "/lustre/scratch122/tol/teams/blaxter/users/am75/assemblies/dataset/ilOpeBrum1.1_genomic.fna.fai") //nolint:lll
				}

				i++
			}
			So(i, ShouldEqual, 18890)
		})

		Convey("you can get extract info for files older than the specified age", func() {
			p.FilterForFilesOlderThan(yearsRelativeToTestFileCreation(7))

			i := 0
			for p.Scan() {
				if i == 0 {
					So(string(p.Path), ShouldEqual, "/lustre/scratch122/tol/teams/blaxter/users/cc51/software/samtools-1.9/htslib-1.9/hfile_net.c") //nolint:lll
					So(p.Size, ShouldEqual, 3192)
					So(p.GID, ShouldEqual, 15078)
					So(p.MTime, ShouldEqual, 1437483022)
					So(p.CTime, ShouldEqual, 1703699980)
				} else if i == 1 {
					So(string(p.Path), ShouldEqual, "/lustre/scratch122/tol/teams/blaxter/users/cc51/software/bcftools-1.19/test/view.filter.10.out") //nolint:lll
					So(p.Size, ShouldEqual, 3754)
					So(p.MTime, ShouldEqual, 1402590965)
				}

				i++
			}
			So(i, ShouldEqual, 6)
		})

		Convey("the age filter gives different results with different ages", func() {
			p.FilterForFilesOlderThan(yearsRelativeToTestFileCreation(6))

			i := 0
			for p.Scan() {
				i++
			}
			So(i, ShouldEqual, 9)
		})
	})
}

func yearsRelativeToTestFileCreation(years int) time.Duration {
	timeDifference := time.Since(time.Unix(epochWhenTestFileWasCreated, 0))
	yearsDifference := time.Duration(years) * 365 * 24 * time.Hour

	return timeDifference + yearsDifference
}

func BenchmarkScanAndFileInfo(b *testing.B) {
	tempDir := b.TempDir()
	testStatsFile := filepath.Join(tempDir, "test.stats")

	f, gr := openTestFile(b)

	defer f.Close()
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

		p.FilterForFilesOlderThan(yearsRelativeToTestFileCreation(7))

		for p.Scan() {
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

func openTestFile(b *testing.B) (io.ReadCloser, io.ReadCloser) {
	f, err := os.Open("test.stats.gz")
	if err != nil {
		b.Fatal(err)
	}

	gr, err := gzip.NewReader(f)
	if err != nil {
		b.Fatal(err)
	}

	return f, gr
}

func BenchmarkRawScanner(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.StopTimer()

		f, gr := openTestFile(b)

		b.StartTimer()

		scanner := bufio.NewScanner(gr)

		for scanner.Scan() {
		}

		gr.Close()
		f.Close()
	}
}

func BenchmarkRawScannerUncompressed(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.StopTimer()

		f, gr := openTestFile(b)

		b.StartTimer()

		scanner := bufio.NewScanner(f)

		for scanner.Scan() {
		}

		gr.Close()
		f.Close()
	}
}
