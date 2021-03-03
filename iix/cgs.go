package iix

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"reflect"
	"sort"
	"strings"

	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"
)

type lessFunc func(int, int) bool

func finish(i1, i2 int) bool { return false }

type jointColumnStats struct {
	Bs       *roaring.Bitmap `json:"hState"`
	HColumns []string        `json:"hColumns"`
	IColumns []string        `json:"iColumns"`
}

type jointColumns []jointColumnStats

type columnGroupStats struct {
	id       string
	JColumns jointColumns `json:"jointColumns"`
	HRows    int          `json:"hRows"`
	IRows    int          `json:"iRows"`
	HRowsAgg int
	IRowsAgg int
}
type columnGroups []columnGroupStats

type cgShoulderIndexes struct {
	jIndex int
	hIndex int
	iIndex int
}
type cgAggregationIndexes struct {
	sub   cgShoulderIndexes
	super cgShoulderIndexes
}

func (jcs jointColumns) byHColumnLen(nextFunc lessFunc) lessFunc {
	return lessFunc(func(i int, j int) bool {
		eq := len(jcs[i].HColumns) == len(jcs[j].HColumns)
		if eq {
			return nextFunc(i, j)
		}
		return len(jcs[i].HColumns) < len(jcs[j].HColumns)
	})
}

func (jcs jointColumns) byIColumnLen(nextFunc lessFunc) lessFunc {
	return lessFunc(func(i int, j int) bool {
		eq := len(jcs[i].IColumns) == len(jcs[j].IColumns)
		if eq {
			return nextFunc(i, j)
		}
		return len(jcs[i].IColumns) < len(jcs[j].IColumns)
	})
}

func (cgs columnGroups) byJColumnLen(nextFunc lessFunc) lessFunc {
	return lessFunc(func(i int, j int) bool {
		eq := len(cgs[i].JColumns) == len(cgs[j].JColumns)
		if eq {
			return nextFunc(i, j)
		}
		return len(cgs[i].JColumns) < len(cgs[j].JColumns)
	})
}

func (cgs columnGroups) byTotalColumns(nextFunc lessFunc) lessFunc {
	return lessFunc(func(i int, j int) bool {
		iCount := 0
		jCount := 0
		for _, z := range cgs[i].JColumns {
			iCount += len(z.HColumns) + len(z.IColumns)
		}
		for _, z := range cgs[j].JColumns {
			jCount += len(z.HColumns) + len(z.IColumns)
		}
		eq := iCount == jCount
		if eq {
			return nextFunc(i, j)
		}
		return iCount < jCount
	})
}

func (cg columnGroupStats) PrintResult() {
	hcc, icc, uq := 0, 0, 0

	for i := range cg.JColumns {
		if cg.HRowsAgg == 0 {
			cg.HRows = cg.HRowsAgg
		}
		if cg.IRowsAgg == 0 {
			cg.IRows = cg.IRowsAgg
		}
		uq += int(cg.JColumns[i].Bs.GetCardinality())
		hcc += len(cg.JColumns[i].HColumns)
		icc += len(cg.JColumns[i].HColumns)
		//uq += math.Round(float64(100.0) * float64(cg.JColumns[i].Bs.GetCardinality()) / float64(cg.HRowsAgg))
	}
	//uq = uq / float64(100*len(cg.JColumns))

	fmt.Printf("%4v  |%6v|%6v|%6v|%6v|%7v|%2v|%2v|%2v| ",
		cg.id, cg.HRows, cg.IRows,
		cg.HRowsAgg, cg.IRowsAgg,
		uq, len(cg.JColumns), hcc, icc,
	)
	for i := range cg.JColumns {
		fmt.Printf("[%v/%v/%v]",
			strings.Join(cg.JColumns[i].HColumns, ","),
			cg.JColumns[i].Bs.GetCardinality(),
			strings.Join(cg.JColumns[i].IColumns, ","),
		)
	}
	fmt.Println()
}

func (sbs columnGroupStats) IsSubSetOf(sps columnGroupStats) []cgAggregationIndexes {
	return sps.IsSuperSetOf(sbs)
}

func (sps columnGroupStats) IsSuperSetOf(sbs columnGroupStats) []cgAggregationIndexes {
	if len(sps.id) > 0 && sps.id == sbs.id {
		return nil
	}
	if len(sps.JColumns) < len(sbs.JColumns) {
		return nil
	}
	spsMap := make(map[string]cgShoulderIndexes)
	mapDelimiter := 1

	for ji := range sps.JColumns {
		for hi := range sps.JColumns[ji].HColumns {
			for ii := range sps.JColumns[ji].IColumns {
				k := fmt.Sprintf("%v%v%v", sps.JColumns[ji].HColumns[hi], mapDelimiter, sps.JColumns[ji].IColumns[ii])
				spsMap[k] = cgShoulderIndexes{
					jIndex: ji,
					hIndex: hi,
					iIndex: ii,
				}
			}
		}
	}
	result := make([]cgAggregationIndexes, 0, len(spsMap))
	for ji := range sbs.JColumns {
		for hi := range sbs.JColumns[ji].HColumns {
			for ii := range sbs.JColumns[ji].IColumns {
				k := fmt.Sprintf("%v%v%v", sbs.JColumns[ji].HColumns[hi], mapDelimiter, sbs.JColumns[ji].IColumns[ii])
				spsIx, xFound := spsMap[k]
				if !xFound {
					// When a single entry not found - the sbs is not a subset of sps
					return nil
				}
				result = append(result, cgAggregationIndexes{
					super: spsIx,
					sub: cgShoulderIndexes{
						jIndex: ji,
						hIndex: hi,
						iIndex: ii,
					},
				})
			}
		}
	}
	return result
}

type compMwrFunc func(f lessFunc) lessFunc

func (cgs columnGroups) hRows(nextFunc lessFunc) lessFunc {
	return lessFunc(func(i int, j int) bool {
		eq := cgs[i].HRows == cgs[j].HRows
		if eq {
			return nextFunc(i, j)
		}
		return cgs[i].HRows < cgs[j].HRows
	})
}

func (gss columnGroups) Sort() {

	hRows := func(cf lessFunc) lessFunc {
		return lessFunc(func(i int, j int) bool {
			eq := gss[i].HRows == gss[j].HRows
			if eq {
				return cf(i, j)
			}
			return gss[i].HRows < gss[j].HRows
		})
	}
	iRows := func(cf lessFunc) lessFunc {
		return lessFunc(func(i int, j int) bool {
			eq := gss[i].IRows == gss[j].IRows
			if eq {
				return cf(i, j)
			}
			return gss[i].IRows < gss[j].IRows
		})
	}

	jColumns := func(cf lessFunc) lessFunc {
		return lessFunc(func(i int, j int) bool {
			eq := len(gss[i].JColumns) == len(gss[j].JColumns)
			if eq {
				return cf(i, j)
			}
			return len(gss[i].JColumns) > len(gss[j].JColumns)
		})
	}

	hColumn := func(cf lessFunc) lessFunc {
		return lessFunc(func(i int, j int) bool {
			eq := len(gss[i].JColumns[0].HColumns) == len(gss[j].JColumns[0].HColumns)
			if eq {
				for n := range gss[i].JColumns[0].HColumns {
					eq = eq || (gss[i].JColumns[0].HColumns[n] == gss[j].JColumns[0].HColumns[n])
					if !eq {
						return eq
					}
				}
				return cf(i, j)
			}
			return len(gss[i].JColumns[0].HColumns) < len(gss[j].JColumns[0].HColumns)
		})
	}

	iColumn := func(cf lessFunc) lessFunc {
		return lessFunc(func(i int, j int) bool {
			eq := len(gss[i].JColumns[0].IColumns) == len(gss[j].JColumns[0].IColumns)
			if eq {
				for n := range gss[i].JColumns[0].IColumns {
					eq = eq || (gss[i].JColumns[0].IColumns[n] == gss[j].JColumns[0].IColumns[n])

					if !eq {
						return eq
					}
				}
				return cf(i, j)
			}
			return len(gss[i].JColumns[0].IColumns) < len(gss[j].JColumns[0].IColumns)
		})
	}

	//gss[i].JColumns[0].HColumns[0] == gss[j].JColumns[0].HColumns[0]

	shoulder := func(cf lessFunc) lessFunc {
		return lessFunc(func(i int, j int) bool {
			iShoulder := float64(0)
			jShoulder := float64(0)
			for ii := range gss[i].JColumns {
				iShoulder += math.Round(float64(100.0) * float64(gss[i].JColumns[ii].Bs.GetCardinality()) / float64(gss[i].HRows))
			}
			for ii := range gss[j].JColumns {
				jShoulder += math.Round(float64(100) * float64(gss[j].JColumns[ii].Bs.GetCardinality()) / float64(gss[j].HRows))
			}
			iShoulder = iShoulder / float64(100*len(gss[i].JColumns))
			jShoulder = jShoulder / float64(100*len(gss[j].JColumns))
			eq := iShoulder == jShoulder
			if eq {
				return cf(i, j)
			}
			return iShoulder < jShoulder
		})
	}
	_ = shoulder

	sort.Slice(gss, hRows(iRows(jColumns(hColumn(iColumn(finish))))))

	/*
		sort.Slice(gss, func(i, j int) bool {
			iShoulder := float64(0)
			jShoulder := float64(0)
			for ii := range gss[i].JColumns {
				iShoulder += math.Round(float64(100.0) * float64(gss[i].JColumns[ii].Bs.GetCardinality()) / float64(gss[i].HRows))
			}
			for ii := range gss[j].JColumns {
				jShoulder += math.Round(float64(100) * float64(gss[j].JColumns[ii].Bs.GetCardinality()) / float64(gss[j].HRows))
			}
			iShoulder = iShoulder / float64(100*len(gss[i].JColumns))
			jShoulder = jShoulder / float64(100*len(gss[j].JColumns))
			if iShoulder == jShoulder {
				if gss[i].JColumns[0].HColumns[0] == gss[j].JColumns[0].HColumns[0] {
					return gss[i].HRows > gss[j].HRows
				}
				return gss[i].JColumns[0].HColumns[0] < gss[j].JColumns[0].HColumns[0]
			}
			return iShoulder > jShoulder
		},
		)
		for _, gs := range gss {
			sort.Slice(gs.JColumns, func(i, j int) bool {
				if gs.JColumns[i].Bs.GetCardinality() == gs.JColumns[j].Bs.GetCardinality() {
					gs.JColumns[i].Bs.GetCardinality() > gs.JColumns[j].Bs.GetCardinality()
				}
				return gs.JColumns[i].Bs.GetCardinality() > gs.JColumns[j].Bs.GetCardinality()
			})
		}*/

}

func (gss columnGroups) PrintResult() {
	//gss.Sort()
	/*	for n := range gs.JColumns {
			sort.Slice(gs.JColumns[n].HColumns, func(i, j int) bool {
				return gs.JColumns[n].HColumns[i] < gs.JColumns[n].HColumns[j]
			})
			sort.Slice(gs.JColumns[n].IColumns, func(i, j int) bool {
				return gs.JColumns[n].IColumns[i] < gs.JColumns[n].IColumns[j]
			})
		}
	*/
	for _, gs := range gss {
		gs.PrintResult()
	}
}

func (g columnGroupStats) WriteTo(w io.Writer) (writtenBytes int, err error) {
	const uint64Size int = 8
	//	var bufferSize int
	type strategyFunc = func(interface{}) (int, error)

	// totalSizeCollector := func(iv interface{}) (int, error) {
	// 	switch v := iv.(type) {
	// 	case int:
	// 		{
	// 			return uint64Size, nil
	// 		}
	// 	case string:
	// 		{
	// 			return uint64Size + len(v), nil
	// 		}
	// 	case *roaring.Bitmap:
	// 		{
	// 			return uint64Size + int(v.GetSerializedSizeInBytes()), nil
	// 		}
	// 	case interface{}:
	// 		{
	// 			rt := reflect.TypeOf(iv)
	// 			rk := rt.Kind()
	// 			switch rk {
	// 			case reflect.Slice, reflect.Array:
	// 				{
	// 					return uint64Size, nil
	// 				}
	// 			default:
	// 				return -1, fmt.Errorf("unhandled type occurred: %T", iv)
	// 			}
	// 		}
	// 	}
	// 	return -1, fmt.Errorf("unhandled type occurred: %T", iv)
	// }

	bufferWriter := func(iv interface{}) (n int, err error) {
		var order = binary.LittleEndian

		switch v := iv.(type) {

		case int:
			{
				n = uint64Size
				err = binary.Write(w, order, int64(v))
				return
			}
		case string:
			{
				err = binary.Write(w, order, int64(len(v)))
				n, err = w.Write([]byte(v))
				n += uint64Size
				return
			}
		case *roaring.Bitmap:
			{
				// var n1 int
				// var n2 int64
				// n1, err = binary.Write(w,order,v.GetSerializedSizeInBytes())
				// if err != nil {
				// 	return n1, err
				// }
				// n2, err = v.WriteTo(w)
				// n1 += int(n2)
				// return n1, err
				var n64 int64
				n64, err = v.WriteTo(w)
				n = int(n64)
				return
			}
		case interface{}:
			{
				rt := reflect.TypeOf(iv)
				kt := rt.Kind()
				switch kt {
				case reflect.Slice, reflect.Array:
					{
						rv := reflect.ValueOf(iv)
						n = uint64Size
						err = binary.Write(w, order, int64(rv.Len()))
						return
					}
				default:
					err = fmt.Errorf("unhandled interface type occurred: %T", iv)
					return
				}
			}
		}
		err = fmt.Errorf("unhandled type occurred: %T", iv)
		return
	}

	traverse := func(sf strategyFunc) (total int, err error) {
		var n int
		// n, err = sf("cgs")
		// if err != nil {
		// 	return -1, errors.Errorf("could not process the 'cgs' magic word. %w", err)
		// }
		// total += n

		// n, err = sf(bufferSize)
		// total += n
		n, err = sf(g.id)
		if err != nil {
			err = errors.Errorf("could not process g.id. %w", err)
			return
		}

		total += n
		n, err = sf(g.HRows)
		if err != nil {
			err = errors.Errorf("could not process g.hRows. %w", err)
			return
		}
		total += n

		n, err = sf(g.IRows)
		if err != nil {
			errors.Errorf("could not process g.iRows. %w", err)
			return
		}
		total += n

		n, err = sf(g.JColumns)
		if err != nil {
			err = errors.Errorf("could not process g.JColumns. %w", err)
			return
		}
		total += n

		for i := range g.JColumns {
			n, err = sf(g.JColumns[i].HColumns)
			total += n
			for j := range g.JColumns[i].HColumns {
				n, err = sf(g.JColumns[i].HColumns[j])
				total += n
			}
			n, err = sf(g.JColumns[i].IColumns)
			total += n
			for j := range g.JColumns[i].IColumns {
				n, err = sf(g.JColumns[i].IColumns[j])
				total += n
			}
			n, err = sf(g.JColumns[i].Bs)
			total += n
		}
		return total, nil
	}

	// bufferSize, err = traverse(totalSizeCollector)
	// if err != nil {
	// 	return -1, err
	// }

	writtenBytes, err = traverse(bufferWriter)
	if err != nil {
		return -1, err
	}
	return
}
func (g *columnGroupStats) ReadFrom(r io.Reader) (readSize int, err error) {
	nextInt := func() (v int, readSize int, err error) {
		const uint64Size int = 8
		var n int
		uintBuff := [uint64Size]byte{}
		n, err = r.Read(uintBuff[:])
		readSize += n
		if err != nil {
			err = errors.Wrap(err, "could not read bytes for int value")
			return
		}
		v = int(binary.LittleEndian.Uint64(uintBuff[:]))
		return
	}

	nextByteSlice := func() (v []byte, readSize int, err error) {
		var n, l int
		l, n, err = nextInt()
		readSize += n
		if err != nil {
			err = errors.Wrap(err, "reading byte slice length")
			return
		}
		v = make([]byte, l)
		n, err = r.Read(v)
		readSize += n
		if err != nil {
			err = errors.Wrapf(err, "reading slice of [%v]bytes", l)
			return
		}
		return
	}

	nextString := func() (v string, readSize int, err error) {
		var b []byte
		b, readSize, err = nextByteSlice()
		defer func() {
			if b != nil {
				v = string(b)
			}
		}()

		if err != nil {
			return
		}
		if b == nil {
			err = errors.New("getting a string value: slice is null")
			return
		}
		return
	}
	nextStringSlice := func() (ss []string, readSize int, err error) {
		var n, l int
		l, n, err = nextInt()
		readSize += n
		if err != nil {
			err = errors.Wrap(err, "reading length of a string slice")
			return
		}
		ss = make([]string, l)
		for i := range ss {
			var s string
			s, n, err = nextString()
			readSize += n
			if err != nil {
				err = errors.Wrap(err, "reading value of a string in a slice")
				return
			}
			ss[i] = s
		}
		return
	}

	var n, l int
	// n, err = r.Read(pwdBuff[:])
	// readSize += n
	// if err != nil {
	// 	if err == io.EOF {
	// 		return
	// 	}
	// 	return -1, errors.Errorf("read pwd: %w", err)
	// }
	// if string(pwdBuff[:]) != "cgs" {
	// 	return -1, errors.New("magic word is wrong")
	// }

	// totalSize, n,err = nextInt()
	// readSize += n
	// if err != nil {
	// 	err =  errors.Errorf("read totalSize: %w", err)
	// 	return
	// }
	g.id, n, err = nextString()
	readSize += n
	if err != nil {
		err = errors.Wrap(err, "reading id")
		return
	}

	g.HRows, n, err = nextInt()
	readSize += n
	if err != nil {
		err = errors.Wrap(err, "reading HRows")
		return
	}

	g.IRows, n, err = nextInt()
	readSize += n
	if err != nil {
		err = errors.Wrap(err, "reading IRows")
		return
	}

	l, n, err = nextInt()
	readSize += n
	if err != nil {
		err = errors.Wrap(err, "reading length of JColumns: %w")
		return
	}

	g.JColumns = make([]jointColumnStats, l)
	for i := range g.JColumns {
		g.JColumns[i].HColumns, n, err = nextStringSlice()
		readSize += n
		if err != nil {
			err = errors.Wrap(err, "reading HColumns")
			return
		}
		g.JColumns[i].IColumns, n, err = nextStringSlice()
		readSize += n
		if err != nil {
			err = errors.Wrap(err, "reading IColumns")
			return
		}
		g.JColumns[i].Bs = roaring.NewBitmap()
		var n64 int64
		n64, err = g.JColumns[i].Bs.ReadFrom(r)
		readSize += int(n64)
		if err != nil {
			err = errors.Wrap(err, "getting bitset state from buffer")
			return
		}
	}
	g.HRowsAgg = g.HRows
	g.IRowsAgg = g.IRows

	return
}

type ColumnGroupAggregator struct {
	cgs columnGroups
}

func (cga *ColumnGroupAggregator) ReadFrom(r io.Reader) (n int, err error) {
	cga.cgs = make(columnGroups, 0, 100) //TODO:
	for {
		cg := new(columnGroupStats)
		_, err = cg.ReadFrom(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Fatalf("reading precomputed: %w", err)
		}
		cga.cgs = append(cga.cgs, *cg)
	}
	cga.cgs.PrintResult()
	return
}

func (a ColumnGroupAggregator) Aggregate() (r ColumnGroupAggregator) {
	type mergeRef struct {
		cgsIndex        int
		shoulderIndexes []cgAggregationIndexes
	}

	mrCache := make(map[string]mergeRef)

	inCache := func(i, j int) (mr mergeRef, found bool) {
		key := fmt.Sprintf("%v/%v", i, j)
		if mr, found = mrCache[key]; !found {
			mr = mergeRef{
				cgsIndex:        j,
				shoulderIndexes: a.cgs[i].IsSubSetOf(a.cgs[j]),
			}
			mrCache[key] = mr
		}
		return
	}

	isSubsetOf := func(i, j int) (mr mergeRef, isSubset bool) {
		var found bool
		if mr, found = inCache(i, j); !found {
			if len(mr.shoulderIndexes) > 0 {
				if rr, _ := inCache(j, i); len(rr.shoulderIndexes) > 0 {
					a.cgs[i].PrintResult()
					a.cgs[j].PrintResult()
					log.Fatalf("Mutual subset discovered i.id, j.id = %v,%v", a.cgs[i].id, a.cgs[j].id)
				}
			}
		}
		isSubset = len(mr.shoulderIndexes) > 0
		return
	}

	sort.Slice(
		a.cgs,
		a.cgs.byJColumnLen(a.cgs.byTotalColumns(finish)),
	)

	for i := range a.cgs {
		sort.Slice(
			a.cgs[i].JColumns,
			a.cgs[i].JColumns.byHColumnLen(a.cgs[i].JColumns.byIColumnLen(finish)),
		)
	}

	_ = isSubsetOf
	fmt.Printf("\nSorted:------------------------------------------\n")

	a.cgs.PrintResult()

	fmt.Printf("\nProcessed------------------------------------------\n")
	aggMap := make(map[int]int)
	merged := make(map[int]int)

	aggFunc := func(i, j int) bool {
		if fromRef, isSubset := isSubsetOf(i, j); isSubset {
			aggMap[i]++
			merged[j]=1
			to := &(a.cgs[i])
			to.HRowsAgg += a.cgs[j].HRows
			to.IRowsAgg += a.cgs[j].IRows
			for z := range fromRef.shoulderIndexes {
				spj := fromRef.shoulderIndexes[z].super.jIndex
				sbj := fromRef.shoulderIndexes[z].sub.jIndex
				to.JColumns[sbj].Bs.Or(a.cgs[j].JColumns[spj].Bs)
			}
			return true
		}
		return false
	}
	for i, from := range a.cgs {
		fmt.Printf("\n")
		from.PrintResult()
		for j := i + 1; j < len(a.cgs); j++ {
			if aggregated := aggFunc(i, j); aggregated {
				fmt.Printf("    + ")
				a.cgs[j].PrintResult()
				continue
			}
			if len(a.cgs[i].JColumns) == len(a.cgs[j].JColumns) {
				if aggregated := aggFunc(j, i); aggregated {
					fmt.Printf("    + ")
					a.cgs[i].PrintResult()
				}
				continue
			}
			fmt.Printf("    - ")
			a.cgs[j].PrintResult()
		}
		fmt.Printf("ID %v is done\n", from.id)
	}

	fmt.Printf("\nNot aggregated------------------------------------------\n")
	for i := range a.cgs {
		if _, found := aggMap[i]; !found {
			a.cgs[i].PrintResult()
		}
	}

	fmt.Printf("\nAggregated------------------------------------------\n")
	for i, cnt := range aggMap {
		fmt.Printf("%4v|", cnt)
		a.cgs[i].PrintResult()
	}

	//cutIxs := make(map[int][]int)
	//lostIxs := make(map[int]struct{})
	/*allSupCgsRefs := make(map[int][]subCgsRef)



	for i := range a.cgs {
		subCgsRefs := make([]subCgsRef, 0, 10)
		for j := range a.cgs {
			if ref, found := isSuperSetOf(i,j); found {
				subCgsRefs = append(subCgsRefs, subCgsRef{
					cgsIndex:        j,
					shoulderIndexes: ixs,
				})
			}
		}
		sort.Slice(subCgsRefs,func(i, j int) bool {
			return len(a.cgs[subCgsRefs[i].cgsIndex]) > len(a.cgs[subCgsRefs[j].cgsIndex])
		})
		for si, refI := range subCgsRefs{
			jcLenI = len(a.cgs[refI.cgsIndex].JColumns)
			for sj := si + 1; sj<len(subCgsRefs);sj++ {
				refJ = subCgsRefs[sj]
				if _, f :=isSuperSetOf(refI.cgsIndex, refJ.cgsIndex); !f

				jcLenJ = len(a.cgs[refJ.cgsIndex].JColumns)

				a.cgs[jc]
				if jcLenI> jcLen
			}
		}

				if subCgsRefs, found := allSubCgsRefs[i]; found {
					subCgsRefs = append(subCgsRefs, subCgsRef{
						cgsIndex:        j,
						shoulderIndexes: ixs,
					})
					allSubCgsRefs[i] = subCgsRefs
				} else {
					subCgsRefs = make([]subCgsRef, 0, 10)
					subCgsRefs = append(subCgsRefs, subCgsRef{
						cgsIndex:        j,
						shoulderIndexes: ixs,
					})
					allSubCgsRefs[i] = subCgsRefs
				}
			}
		}
	}
	//Checking mutual subsetting
	for topCgsIndex, topSubCgsRefs := range allSubCgsRefs {
		for _, topRef := range topSubCgsRefs {
			if nextSubCgsRefs, found := allSubCgsRefs[topRef.cgsIndex]; found {
				for _, nextRef := range nextSubCgsRefs {
					if nextRef.cgsIndex == topCgsIndex {
						fmt.Printf("\n----------------------------------------------\n")
						a.cgs[topCgsIndex].PrintResult()
						fmt.Printf("\n<=>\n")
						a.cgs[nextRef.cgsIndex].PrintResult()
					}
				}
			}
		}
	}
	fmt.Printf("\nNo subsets:------------------------------------------\n")
	for i := range a.cgs {
		if _, f := allSubCgsRefs[i]; !f {
			a.cgs[i].PrintResult()
		}
	}
	fmt.Printf("\n#Subsets------------------------------------------\n")

	for topCgsIndex, topSubCgsRefs := range allSubCgsRefs {
		jCgsIndexes := make(map[int]struct{})
		a.cgs[topCgsIndex].PrintResult()
		for _, topRef := range topSubCgsRefs {
			fmt.Print("     >")
			a.cgs[topRef.cgsIndex].PrintResult()
			if nextSubCgsRefs, found := allSubCgsRefs[topRef.cgsIndex]; found {
				for _, nextRef := range nextSubCgsRefs {
					jCgsIndexes[nextRef.cgsIndex] = struct{}{}
					fmt.Print("     >")
					a.cgs[nextRef.cgsIndex].PrintResult()
				}
			}
		}
		newRefs := make([]subCgsRef, 0, len(topSubCgsRefs))
		for _, ref := range topSubCgsRefs {
			if _, found := jCgsIndexes[ref.cgsIndex]; !found {
				newRefs = append(newRefs, ref)
				fmt.Print("     #")
				a.cgs[ref.cgsIndex].PrintResult()
			}
		}
		allSubCgsRefs[topCgsIndex] = newRefs
	}*/

	/*for topCgsIndex, topSubsetRefs := range allSubsets {

		subsetRefs := allSubsets[cgsIndex]
		fmt.Printf("%4v", len(subs))
		from := a.cgs[groupIndex]
		from.PrintResult()
		// extract 1st subset level only
		subSlIndexes:= make([int]struct{})
		for si := range subset[] {
			subSlIndexes[si] = struct{}{}
		}
		for si := range subs
	}



	OrderedSubsets := make([]int, 0, len(allSubsets))
	for groupIndex := range subsets {
		sbsa = append(sbsa, groupIndex)
	}
	//	fmt.Println(sbsa)
	sort.Slice(sbsa, func(i, j int) bool {
		return len(subsets[sbsa[i]]) > len(subsets[sbsa[j]])
	})
	//	fmt.Println(sbsa)

	for _, cgsIndex := range sbsa {
		subsetRefs := subsets[cgsIndex]
		fmt.Printf("%4v", len(subs))
		from := a.cgs[groupIndex]
		from.PrintResult()
		// extract 1st subset level only
		subSlIndexes:= make([int]struct{})
		for si := range subset[] {
			subSlIndexes[si] = struct{}{}
		}
		for si := range subs
	}



			fmt.Print("     >")
			to := a.cgs[subs[si].groupIndex]
			to.PrintResult()
			to.HRows += from.HRows
			to.IRows += from.IRows
			for z := range sh.shoulderIndexes {
				spj := sh.shoulderIndexes[z].super.jIndex
				sbj := sh.shoulderIndexes[z].sub.jIndex

				// sph := sh.shoulderIndexes[z].super.hIndex
				// sbh := sh.shoulderIndexes[z].sub.hIndex

				// spi := sh.shoulderIndexes[z].super.iIndex
				// sbi := sh.shoulderIndexes[z].sub.iIndex


				to.JColumns[sbj].Bs.Or(from.JColumns[spj].Bs)
				fmt.Print("    +>")
				to.PrintResult()

				//sh.shoulderIndexes[z].sub.jIndex

			}
			//			fmt.Print("     +>")

		}
		fmt.Println()
	}
	*/
	/*for ci := range cutIxs {
		cutIndexShown := false
		for i := range a.cgs {
			if _, inCuts := cutIxs[i]; !inCuts {
				aggDetails := a.cgs[ci].IsSuperSetOf(a.cgs[i])
				if aggDetails != nil {
					if !cutIndexShown {
						fmt.Printf("\n----------------------------------------------\n")
						a.cgs[ci].PrintResult()
						fmt.Printf("----------------------------------------------\n")
						cutIndexShown = true
					}
					a.cgs[i].PrintResult()
				} else {
					lostIxs[ci] = struct{}{}
				}
			}
		}
	}
	fmt.Printf("\n\n NOT AGGREGATED: (%v out of %v)\n", len(lostIxs), len(a.cgs))
	for ci := range lostIxs {
		a.cgs[ci].PrintResult()
	}
	*/
	return a
}
