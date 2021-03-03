package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"

	"github.com/ovlad32/hjb/iix"
	"github.com/ovlad32/hjb/iixs"
	"github.com/ovlad32/hjb/meta"
	"github.com/ovlad32/hjb/sources"
)

/*
go test -memprofile mem.out
go tool pprof perftest00.test mem.out

go test -cpuprofile cpu.out
go tool pprof perftest00.test cpu.out

*/

var mode string

func init() {
	flag.StringVar(&mode, "mode", "", "usage")
	flag.Parse()
}
func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	//go BrushDiscoveryGo()
	//rfn, rightColumnNames = getDumpInfo("mt")
	switch strings.ToLower(mode) {
	case "index":
		{
			ix := iix.NewIndex(
				iixs.NewMemStorage(),
				iixs.NewRnStorageFactory(),
			)
			var suffix = "" // "_01"
			{
				dumpDesc := meta.GetDumpDesc("cra" + suffix)

				ts := meta.NewTableService(dumpDesc)

				log.Println("start indexing")
				fl, err := os.OpenFile(dumpDesc.Path, os.O_RDONLY, 0x444)
				if err != nil {
					log.Fatal(err)
				}
				rh := iix.NewRowHandler(ts, ix)
				_, err = sources.TextStream(fl, dumpDesc.ColumnSep, rh)
				if err != nil {
					log.Fatal(err)
				}
				log.Println("finish indexing")
			}

			//"2203.6983.6265.6101"
			//999660920
			// if rcs, found, err := ix.Find("2203.6983.6265.6101"); found {
			// 	if err != nil {
			// 		log.Fatal(err)
			// 	}
			// 	for i, cl := range rcs.Columns() {
			// 		fmt.Printf("Column: %v \n", cl)
			// 		it := rcs.RowIterator(i)
			// 		for it.HasNext() {
			// 			rn := it.Next()
			// 			fmt.Printf("row#: %v,", rn)
			// 		}
			// 	}
			// }

			{
				dumpDesc := meta.GetDumpDesc("mt" + suffix)

				ts := meta.NewTableService(dumpDesc)

				log.Println("start discovery")
				fl, err := os.OpenFile(dumpDesc.Path, os.O_RDONLY, 0x444)
				if err != nil {
					log.Fatal(err)
				}
				rh := iix.NewRowHandler2(ts, ix)
				_, err = sources.TextStream(fl, dumpDesc.ColumnSep, rh)
				if err != nil {
					log.Fatal(err)
				}
				log.Println("finish discovery")
				rh.PrintResult()
				fout, err := os.Create("./result.bin")
				if err != nil {
					log.Fatal(err)
				}
				defer fout.Close()
				_, err = rh.WriteTo(fout)
				if err != nil {
					log.Fatal(err)
				}
			}
		}
	case "agg":
		{
			fr, err := os.Open("./result.bin")
			if err != nil {
				log.Fatal(err)
			}
			defer fr.Close()

			agg := new(iix.ColumnGroupAggregator)
			agg.ReadFrom(fr)
			fmt.Printf("===================================\n\n\n")
			agg.Aggregate();
		}
	default:
		flag.PrintDefaults()

		log.Fatalf("\n\nunknown mode: %v", mode)
	}

}
