package sources

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/ovlad32/hjb/iix"
)

func TextStream(
	stream io.Reader,
	sep string,
	h iix.IRowValueHandler,
) (lineNumber int, err error) {
	scanner := bufio.NewScanner(stream)
	scanner.Split(bufio.ScanLines)
	//scanner.Buffer(make([]byte,4*1024*1024),4*1024*1024)
	// bufio.MaxScanTokenSize)
	startTime := time.Now()
	tickTime := startTime
	tickLineNumber := 0
	for scanner.Scan() {
		lineNumber++
		if len(scanner.Bytes()) == 0 {
			continue
		}
		//fmt.Println(lineNumber)
		//fmt.Println(rowData)
		bytesInColumns := bytes.Split(scanner.Bytes(), []byte(sep))
		stringsInColumns := make([]string, 0, len(bytesInColumns))
		for _, b := range bytesInColumns {
			stringsInColumns = append(stringsInColumns, string(b))
		}

		err = h.Handle(context.TODO(), lineNumber, stringsInColumns)
		if err != nil {
			//TODO:
			return
		}
		if time.Since(tickTime).Seconds() >= 1 {
			tickTime = time.Now()
			fmt.Printf("Processed %v lines. Speed %v lps\n", lineNumber, lineNumber-tickLineNumber)
			tickLineNumber = lineNumber
		}
	}
	if scanner.Err() != nil {
		log.Fatal(err)
	}
	return
}
