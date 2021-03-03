package meta

import (
	"fmt"
	"strings")



type ITableService interface {
	ColumnName(index int) string
	FusionColumnName(name string, pos int, total int) string
	Split(index int, value string) []string
	Reduce(index int, value string) string
}

type tbs struct {
	dd DumpDesc
}

func NewTableService(dd DumpDesc) ITableService {
	return tbs{dd}
}

func (t tbs) ColumnName(index int) string {
	return t.dd.Columns[index].ColumnName
}

func (t tbs) FusionColumnName(name string, index int, total int) string {
	if index == 0 && total == 1 {
		return name
	} else {
		return name + fmt.Sprintf("[%/%v]", index+1, total)
	}
}

func (t tbs) Split(index int, value string) []string {
	sep := t.dd.Columns[index].FusionSep
	if sep == "" {
		return []string{value}
	} else {
		return strings.Split(value, sep)
	}
}

func (t tbs) Reduce(index int, value string) string {
	lc := t.dd.Columns[index].LeadingChar
	if lc == "" {
		return value
	} else {
		return strings.TrimLeft(value,lc)
	}
}


func (t tbs) Separator() string {
	return t.dd.ColumnSep
}
