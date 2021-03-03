package iixs

import (
	"fmt"
	"sort"
)

type rc struct {
	column string
	rns    IRnStorage
}

type Rcs struct {
	rnsf IRnStorageFactory
	s    []rc
}

func NewRcs(rnsf IRnStorageFactory, cap int) Rcs {
	return Rcs{
		rnsf: rnsf,
		s:    make([]rc, 0, cap),
	}
}

func newRcAddress(column string, rns IRnStorage) rc {
	return rc{
		column,
		rns,
	}
}

func (a rc) addRow(n int) rc {
	a.rns.Add(n)
	return a
}

func (rcs Rcs) Len() int {
	return len(rcs.s)
}
func (rcs Rcs) Less(i, j int) bool {
	return rcs.s[i].column < rcs.s[j].column
}
func (rcs Rcs) Swap(i, j int) {
	rcs.s[i], rcs.s[j] = rcs.s[j], rcs.s[i]
}

func (rcs Rcs) indexOf(c string) (ix int, found bool) {
	ix = sort.Search(len(rcs.s), func(i int) bool {
		return rcs.s[i].column >= c
	})
	// if found indeed
	found = ix < len(rcs.s) && rcs.s[ix].column == c
	return
}

func (rcs Rcs) indexOfWithPanic(c string) (ix int) {
	var found bool
	if ix, found = rcs.indexOf(c); !found {
		panic(fmt.Sprintf("RCS column '%v' has not been found!", c))
	}
	return
}

func (rcs *Rcs) Append(r int, c string) error {
	if len(rcs.s) == 0 {
		rcs.s = append(rcs.s, newRcAddress(c, rcs.rnsf.RnStorage()).addRow(r))
		return nil
	}
	ix, found := rcs.indexOf(c)
	if found {
		rcs.s[ix].addRow(r)
	} else {
		tail := rcs.s[len(rcs.s)-1].column
		rcs.s = append(rcs.s, newRcAddress(c, rcs.rnsf.RnStorage()).addRow(r))
		if tail > c {
			sort.Sort(rcs)
		}
	}
	return nil
}

func (rcs Rcs) Columns() []string {
	c := make([]string, 0, len(rcs.s))
	for i := range rcs.s {
		c = append(c, rcs.s[i].column)
	}
	return c
}
func (rcs Rcs) TotalColumns() int {
	return len(rcs.s)
}

func (rcs Rcs) RowIterator(i int) IRnIterator {
	return rcs.s[i].rns.Iterator()
}
func (rcs Rcs) RowPeeker(i int) IRnPeeker {
	return rcs.s[i].rns.Peeker()
}

func (rcs Rcs) TotalRows(i int) int {
	return int(rcs.s[i].rns.Cardinality())
}

func (rcs Rcs) RowIteratorByColumn(c string) IRnIterator {
	return rcs.RowIterator(rcs.indexOfWithPanic(c))
}

func (rcs Rcs) RowPeekerByColumn(c string) IRnPeeker {
	return rcs.RowPeeker(rcs.indexOfWithPanic(c))
}
func (rcs Rcs) TotalRowsByColumn(c string) int {
	return rcs.TotalRows(rcs.indexOfWithPanic(c))
}
