package iixs

import (
	"github.com/RoaringBitmap/roaring"
)

type rnStorageFactory struct{}

func NewRnStorageFactory() IRnStorageFactory {
	return rnStorageFactory{}
}
func (r rnStorageFactory) RnStorage() IRnStorage {
	return NewRoaringBitmapStorage()
}

type roaringBitmapStorage struct {
	s *roaring.Bitmap
}
type rowNumberIterator struct {
	peekable roaring.IntPeekable
}

func NewRoaringBitmapStorage() roaringBitmapStorage {
	return roaringBitmapStorage{
		s: roaring.NewBitmap(),
	}
}

func (rs roaringBitmapStorage) Add(a int) {
	rs.s.AddInt(a)
}
func (rs roaringBitmapStorage) Iterator() IRnIterator {
	return rowNumberIterator{
		peekable: rs.s.Iterator(),
	}
}

func (rs roaringBitmapStorage) Peeker() IRnPeeker {
	return rowNumberIterator{
		peekable: rs.s.Iterator(),
	}
}

func (rs roaringBitmapStorage) Cardinality() uint {
	return uint(rs.s.GetCardinality())
}

func (ri rowNumberIterator) HasNext() bool {
	return ri.peekable.HasNext()
}

func (ri rowNumberIterator) Next() int {
	return int(ri.peekable.Next())
}
func (ri rowNumberIterator) PeekNext() int {
	return int(ri.peekable.PeekNext())
}
func (ri rowNumberIterator) AdvanceIfNeeded(minValue int) {
	ri.peekable.AdvanceIfNeeded(uint32(minValue))
}
