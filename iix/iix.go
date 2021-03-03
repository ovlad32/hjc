package iix

import (
	"github.com/ovlad32/hjb/iixs"
)

type IIndexer interface {
	Index(value string, rowNumber int, column []string) error
}

type IFinder interface {
	Find(value string) (iixs.Rcs, bool, error)
}
type IIndex interface {
	IIndexer
	IFinder
}

type Index struct {
	s    iixs.IRcStorage
	rnsf iixs.IRnStorageFactory
}

func NewIndex(s iixs.IRcStorage, rnsf iixs.IRnStorageFactory) Index {
	return Index{s, rnsf}
}

func (ix Index) Index(value string, rowNumber int, columns []string) (err error) {
	rcs, indexed, txFunc, err := ix.s.Update(value)
	if err != nil {
		if txFunc != nil {
			txFunc(iixs.ROLLBACK)
		}
		return err
	}
	if !indexed {
		rcs = iixs.NewRcs(ix.rnsf, 1)
	}

	for i := range columns {
		rcs.Append(rowNumber, columns[i])
	}

	err = txFunc(iixs.COMMIT)(rcs)
	if err != nil {
		return err
	}
	return
}

func (ix Index) Find(value string) (rcs iixs.Rcs, found bool, err error) {
	rcs, found, err = ix.s.Find(value)
	return
}
