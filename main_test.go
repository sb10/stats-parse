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
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const firstLineOfTestFile = `L2x1c3RyZS9zY3JhdGNoMTIyL3RvbC90ZWFtcy9ibGF4dGVyL3VzZXJzL2FtNzUvYXNzZW1ibGllcy9kYXRhc2V0L2lsWGVzU2V4czEuMl9nZW5vbWljLmZuYQ==	646315412	21967	15078	1699895920	1698792671	1698917473	f	144116446803265182	1	2983906128` //nolint:lll

func TestSomething(t *testing.T) {
	Convey("Given a parser", t, func() {
		p, err := New("test.stats.gz")
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)

		Convey("You can read the first line", func() {
			scanner := bufio.NewScanner(p.reader)
			scanner.Scan()
			line := scanner.Text()
			So(line, ShouldEqual, firstLineOfTestFile)
			So(scanner.Err(), ShouldBeNil)
		})

		Convey("You can close the file", func() {
			err = p.Close()
			So(err, ShouldBeNil)
		})

		Convey("And an age in years, you can get extract stats for files older than the specified age", func() {
			var i int
			for p.ScanForOldFiles(7) {
				i++

				info := p.FileInfo()
				So(info, ShouldNotBeNil)

				if i == 0 {
					So(info.Path, ShouldEqual, "/lustre/scratch122/tol/teams/blaxter/users/cc51/software/samtools-1.9/htslib-1.9/hfile_net.c") //nolint:lll
					So(info.Size, ShouldEqual, 3192)
					So(info.GID, ShouldEqual, 15078)
					So(info.MTime, ShouldEqual, 1437483022)
					So(info.CTime, ShouldEqual, 1703699980)
				} else if i == 1 {
					So(info.Size, ShouldEqual, 3754)
				}
			}
			So(i, ShouldEqual, 6)
		})
	})

	Convey("openStatsFile returns error if file doesn't exist", t, func() {
		p, err := New("thisdoesntexist")
		So(err, ShouldNotBeNil)
		So(p, ShouldBeNil)
	})
}
