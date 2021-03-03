package iixs

type ITX int

const (
	COMMIT ITX = iota
	ROLLBACK
)

type IRnStorage interface {
	Add(int)
	Cardinality() uint
	Iterator() IRnIterator
	Peeker() IRnPeeker
}

type IRnIterator interface {
	HasNext() bool
	Next() int
}
type IRnPeeker interface {
	IRnIterator
	PeekNext() int
	AdvanceIfNeeded(minval int)
}

type IRnStorageFactory interface {
	RnStorage() IRnStorage
}

type IRcStorageFactory interface {
	RcStorage() IRcStorage
}

type PushFunc = func(Rcs) error
type TxFunc = func(ITX) PushFunc

type IRcUpdater interface {
	Update(string) (Rcs, bool, TxFunc, error)
}
type IRcFinder interface {
	Find(string) (Rcs, bool, error)
}

type IRcStorage interface {
	IRcUpdater
	IRcFinder
}

/*
type IRcs interface {
	Columns() []string
	TotalRows(string) uint
	RowIterator(string) IRnIterator
}
*/

/*
type IAppender interface {
	Append(rn int, cl string) error
}
*/
