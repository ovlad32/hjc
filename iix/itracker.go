package iix

import (
	"math"
	"sort"
	"strings"

	"github.com/ovlad32/hjb/iixs"
)

type indexedRowsTracker struct {
	curRow       int
	iColumnIndex int
	iColumn      string
	nodeIndex    int
	iterator     iixs.IRnPeeker
}

type ixrwTrackers []indexedRowsTracker

func (ts ixrwTrackers) mins() (min1 int, min2 int) {
	if len(ts) == 0 {
		return
	}
	min1 = math.MaxInt64
	min2 = math.MaxInt64
	for i := range ts {
		if ts[i].curRow < 0 {
			continue
		}
		if min1 > ts[i].curRow || min1 == math.MaxInt64 {
			min2 = min1
			min1 = ts[i].curRow
		} else if min1 < ts[i].curRow && (min2 > ts[i].curRow || min2 == math.MaxInt64) {
			min2 = ts[i].curRow
		}
	}
	return
}

func (ts ixrwTrackers) key(rm rKeyer) string {
	/*tsc := make(ixrwTrackers, len(ts))
	copy(tsc, ts)
	sort.Slice(tsc, func(i, j int) bool {
		return tsc[i].iColumn < tsc[j].iColumn
	})
	return rm.keySlice(len(tsc), func(i int) string {
		return tsc[i].iColumn
	})
	*/
	tsc := make([]string, 0, len(ts))
	for j := range ts {
		tsc = append(tsc, ts[j].iColumn)
	}
	sort.Strings(tsc)
	return strings.Join(tsc, "~")
}
func (ts ixrwTrackers) columns() []string {
	cs := make([]string, 0, len(ts))
	for i := range ts {
		cs = append(cs, ts[i].iColumn)
	}
	return cs
}

func (ts indexedRowsTracker) String() string {
	return ts.iColumn
}
