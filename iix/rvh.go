package iix

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/ovlad32/hjb/iixs"
	"github.com/ovlad32/hjb/meta"
)

type IRowValueHandler interface {
	Handle(cx context.Context, rowNumber int, values []string) error
}
type IRowValueHandleResult interface {
	PrintResult()
}
type IRowValueDiscovery interface {
	IRowValueHandler
	IRowValueHandleResult
	WriteTo(w io.Writer) (int, error)
}

type RowDataHandler struct {
	vhc vhc
	ixr IIndexer
}
type vhc struct {
	tbs meta.ITableService
}

type sameValueColumnsMap map[string][]string

func (c vhc) SameValueColumns(values []string) sameValueColumnsMap {
	columnMap := make(sameValueColumnsMap)
	for index, value := range values {
		if value == "" {
			continue
		}
		name := c.tbs.ColumnName(index)
		subValues := c.tbs.Split(index, value)

		for i, subValue := range subValues {
			subValue = c.tbs.Reduce(index, subValue)
			var sameValueColumns []string
			var found bool
			if sameValueColumns, found = columnMap[subValue]; !found {
				sameValueColumns = make([]string, 0, len(subValues))
			}
			subColumnName := c.tbs.FusionColumnName(name, i, len(subValues))
			columnMap[subValue] = append(sameValueColumns, subColumnName)
		}
	}
	return columnMap
}

func NewRowHandler(tbs meta.ITableService, ixr IIndexer) RowDataHandler {
	return RowDataHandler{
		vhc{tbs},
		ixr,
	}
}

func (r RowDataHandler) Handle(cx context.Context, rowNumber int, values []string) error {
	columnMap := r.vhc.SameValueColumns(values)
	for subValue, subColumns := range columnMap {
		err := r.ixr.Index(subValue, rowNumber, subColumns)
		if err != nil {
			//TODO:
			return err
		}
	}

	return nil
}

type rvhd struct {
	vhc           vhc
	ixf           IFinder
	columnNameMap map[string]int
	//xStatsMap     map[string]xStats
	cgsMap map[string]columnGroupStats
	gen    *countHolder
}
type countHolder struct {
	cgsMx  sync.Mutex
	cgsSeq int
	cnmMx  sync.Mutex
	cnmSeq int
}

func (c *countHolder) nextGroupId() int {
	c.cgsMx.Lock()
	c.cgsSeq++
	c.cgsMx.Unlock()
	return c.cgsSeq
}

func (c *countHolder) nextNameId() int {
	c.cnmMx.Lock()
	c.cnmSeq++
	c.cnmMx.Unlock()
	return c.cnmSeq
}

func NewRowHandler2(tbs meta.ITableService, ixf IFinder) IRowValueDiscovery {
	r := rvhd{
		vhc: vhc{tbs},
		ixf: ixf,
	}
	r.internalInit()
	return r
}

func (r *rvhd) internalInit() {
	r.columnNameMap = make(map[string]int)
	//r.xStatsMap = make(map[string]xStats)
	r.cgsMap = make(map[string]columnGroupStats)
	r.gen = new(countHolder)
}

/*
type rnTrack struct {
	rcs iixs.Rcs
	//rcsRnCount[] int
	iColumns []string
	key string
}

func newRnTrack(rcs iixs.Rcs, hColumns []string) rnTrack {
	 t := rnTrack{
		rcs: rcs,
		//rcsRnCount:make([]int,0,rcs.Len()),
		hColumns: hColumns,
		key: misc.BuildMapKey(rcs.Columns(),hColumns,string([]byte{'~'})),
	}
	// for i := range j.rcs.Columns() {
	// 	j.rcsRnCount = append(j.rcsRnCount, rcs.TotalRows(i))
	// }
	return t
}*/

type rKeyer interface {
	keySlice(int, func(int) string) string
}

type valueNode struct {
	hColumns []string
	value    string
	rcs      iixs.Rcs
}

func (n valueNode) key(k rKeyer) string {
	tsc := make([]string, 0, len(n.hColumns))
	for j := range n.hColumns {
		tsc = append(tsc, n.hColumns[j])
	}
	sort.Strings(tsc)
	return strings.Join(tsc, "~")

	/*return k.keySlice(len(n.hColumns), func(i int) string {
		return n.hColumns[i]
	})*/
}

func (n valueNode) columns() []string {
	return n.hColumns
}

type iCoverage struct {
	itracks map[int]ixrwTrackers
	rows    int
}

func (r rvhd) Handle(cx context.Context, rowNumber int, values []string) error {
	columnMap := r.vhc.SameValueColumns(values)
	valueNodes := make([]valueNode, 0, len(columnMap))
	iTrackers := make([]indexedRowsTracker, 0, len(columnMap)) // not exact allocation
	for subValue, subColumns := range columnMap {
		rcs, found, err := r.ixf.Find(subValue)
		if err != nil {
			//TODO:
			return err
		}
		if found {
			sort.Slice(subColumns, func(i, j int) bool {
				return subColumns[i] >= subColumns[j]
			})
			valueNodes = append(valueNodes, valueNode{
				hColumns: subColumns,
				value:    subValue,
				rcs:      rcs,
			})
			//j := newRnTrack(rcs, subColumns)
			//joints = append(joints, j)
			//fmt.Printf("%v in %v\n", subValue, j.key)
		}
	}

	sort.Slice(valueNodes, func(i, j int) bool {
		if len(valueNodes[i].hColumns) == len(valueNodes[j].hColumns) {
			for cIndex := range valueNodes[i].hColumns {
				if valueNodes[i].hColumns[cIndex] < valueNodes[j].hColumns[cIndex] {
					return true
				}
			}
		} else {
			return len(valueNodes[i].hColumns) < len(valueNodes[j].hColumns)
		}
		return false
	})

	for nodeIndex := range valueNodes {
		for i, column := range valueNodes[nodeIndex].rcs.Columns() {
			iTrackers = append(iTrackers, indexedRowsTracker{
				iColumnIndex: i,
				iColumn:      column,
				nodeIndex:    nodeIndex,
				iterator:     valueNodes[nodeIndex].rcs.RowPeeker(i),
			})
		}
	}

	a := fnv.New32()
	icvgMap := collectIntersections(iTrackers)
	if len(icvgMap) > 0 {
		//fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
		for _, cvg := range icvgMap {
			if len(cvg.itracks) > 1 {
				//fmt.Printf("%v : %v\n", cvg.rows, cvg.iTracks)
				jKeys := make([]string, 0, len(cvg.itracks)) //TODO: fix length
				for nodeIndex, tracks := range cvg.itracks {
					iKey := tracks.key(r)
					hKey := valueNodes[nodeIndex].key(r)
					jKeys = append(jKeys, iKey+"-"+hKey)
				}
				sort.Strings(jKeys)
				gKey := strings.Join(jKeys, "/")
				//fmt.Println(gKey)
				if gs, found := r.cgsMap[gKey]; !found {
					gs = columnGroupStats{
						id:       fmt.Sprintf("%v", r.gen.nextGroupId()), // ++ len(r.cgsMap)+1),
						JColumns: make([]jointColumnStats, 0, len(cvg.itracks)),
						IRows:    cvg.rows,
						IRowsAgg: cvg.rows,
						HRows:    1,
						HRowsAgg: 1,
					}
					for nodeIndex, tracks := range cvg.itracks {
						j := jointColumnStats{
							HColumns: valueNodes[nodeIndex].columns(),
							IColumns: tracks.columns(),
							Bs:       roaring.NewBitmap(),
						}
						a.Reset()
						a.Write([]byte(valueNodes[nodeIndex].value))
						j.Bs.Add(a.Sum32())
						gs.JColumns = append(gs.JColumns, j)
					}
					r.cgsMap[gKey] = gs
				} else {
					for nodeIndex, tracks := range cvg.itracks {
						i := 0
						found := false
					outer:
						for ; i < len(gs.JColumns); i++ {
							for j := range gs.JColumns[i].IColumns {
								found = gs.JColumns[i].IColumns[j] == tracks[0].iColumn
								if found {
									break outer
								}
							}
						}
						if !found {
							panic("not found!")
						}
						a.Reset()
						a.Write([]byte(valueNodes[nodeIndex].value))
						gs.JColumns[i].Bs.Add(a.Sum32())
					}
					gs.IRows += cvg.rows
					gs.IRowsAgg += cvg.rows
					gs.HRows++
					gs.HRowsAgg++
					r.cgsMap[gKey] = gs
				}
			}

			/*fmt.Printf("Filtered out:\n")
			for _, cvg := range icvgMap {
				if len(cvg.itracks) == 1 {
					fmt.Printf("%v : %v\n", cvg.rows, cvg.itracks)
				}
			}*/
		}
	} else {
		fmt.Printf("No row intersections discovered\n")
	}

	if rowNumber == -5000 {

		panic(1)
	}
	//fmt.Printf("-----------------------------------------------\n")

	return nil
}
func (r rvhd) mapColumnName(c string) (i int) {
	var found bool
	if i, found = r.columnNameMap[c]; !found {
		i = r.gen.nextNameId()
		r.columnNameMap[c] = i
	}
	return i
}

func (r rvhd) keySlice(maxLen int, getFunc func(i int) string) (key string) {
	maxColumnIndex := 0
	columnIndexes := make(map[int]struct{})
	for i := 0; i < maxLen; i++ {
		ci := r.mapColumnName(getFunc(i))
		if ci > maxColumnIndex {
			maxColumnIndex = ci
		}
		columnIndexes[ci] = struct{}{}
	}
	keyChars := make([]byte, maxColumnIndex)
	for i := range keyChars {
		if _, f := columnIndexes[i+1]; f {
			keyChars[i] = 'T'
		} else {
			keyChars[i] = 'F'
		}
	}
	return string(keyChars)
}

func (r rvhd) PrintResult() {
	gss := make(columnGroups, 0, len(r.cgsMap))
	for _, gs := range r.cgsMap {
		gss = append(gss, gs)
	}
	gss.PrintResult()
}

func (r rvhd) WriteTo(w io.Writer) (total int, err error) {
	var n = 0
	for _, gs := range r.cgsMap {
		n, err = gs.WriteTo(w)
		if err != nil {
			return -1, err
		}
		total += n
	}
	return
}

func collectIntersections(tracks ixrwTrackers) (cvgMap map[string]iCoverage) {
	var KILLED int = -1
	cvgMap = make(map[string]iCoverage)
	columnPositionKey := make([]byte, len(tracks))

	minRn := math.MaxInt64
	nextMinRn := math.MaxInt64

	activeTracksIndexes := make([]int, 0, len(tracks))
	columnsInRow := make(map[int]int)         // rowNumber:coumnCount
	iterators := make(map[int]iixs.IRnPeeker) //trackIndex:Iterator

	killIterator := func(index int) {
		//log.Infof("%v iterator has been exhausted in row #%v", cis[index].column, minRn)
		delete(iterators, index)
		tracks[index].curRow = KILLED
	}

	for index, track := range tracks {
		iterators[index] = track.iterator
		activeTracksIndexes = append(activeTracksIndexes, index)
	}
	for true {
		//Advance Row Iterators if needed
		for _, index := range activeTracksIndexes {
			if !iterators[index].HasNext() {
				killIterator(index)
				continue
			}
			rn := iterators[index].Next()
			tracks[index].curRow = rn
			columnsInRow[rn]++ // increase # columns in the retrieved row number
		}

		if len(iterators) <= 1 {
			break
		}
		// Looking for 1st and second min row numbers
		minRn, nextMinRn = tracks.mins()

		activeTracksIndexes = activeTracksIndexes[:0]

		if columnCount := columnsInRow[minRn]; columnCount > 1 {
			for index := range tracks {
				if tracks[index].curRow == minRn {
					activeTracksIndexes = append(activeTracksIndexes, index)
					columnPositionKey[index] = 'T'
				} else {
					columnPositionKey[index] = 'F'
				}
			}
			k := string(columnPositionKey)
			var cvg iCoverage
			var found bool
			if cvg, found = cvgMap[k]; !found {
				cvg = iCoverage{
					//TODO: the columns to be removed
					//columns: make([]string, 0, columnCount),
					//columnIndexesOnNode: make([]int, 0, columnCount),
					itracks: make(map[int]ixrwTrackers),
					rows:    1,
				}
				var added int = 0
				for index, bit := range columnPositionKey {
					if bit == 'T' {
						//cvg.columnIndexesOnNode  = append(cvg.columnIndexes, tracks[index].columnIndex)
						//cvg.columns  = append(cvg.columns, tracks[index].column)
						//fmt.Println(cis[index].vIndex)
						var nodeTracks ixrwTrackers
						var found bool
						if nodeTracks, found = cvg.itracks[tracks[index].nodeIndex]; !found {
							nodeTracks = make(ixrwTrackers, 0, len(columnPositionKey))
						}
						nodeTracks = append(nodeTracks, tracks[index])
						cvg.itracks[tracks[index].nodeIndex] = nodeTracks
						if added == columnCount {
							break
						}
					}
				}
			} else {
				cvg.rows++
			}
			cvgMap[k] = cvg
		} else {
			for index := range tracks {
				if tracks[index].curRow == minRn {
					activeTracksIndexes = append(activeTracksIndexes, index)
					iterators[index].AdvanceIfNeeded(nextMinRn)
				}
			}
		}

		//fmt.Printf("%v,",minRn)
		delete(columnsInRow, minRn)
	}
	/*coverages := make([]coverage, 0, len(cvgMap))
	for _, cvg := range cvgMap {
		coverages = append(coverages, cvg)
	}*/
	return cvgMap
}
