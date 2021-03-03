package iixs

import (
	"sync"
)

type mMap map[string]Rcs

type memStorage struct {
	mem mMap
}
type mxMemStorage struct {
	mem mMap
	m   sync.RWMutex
}

func NewMemStorage() memStorage {
	return memStorage{
		mem: make(mMap),
	}
}

func (s memStorage) Update(v string) (rcs Rcs, found bool, txFunc TxFunc, err error) {
	txFunc = func(itx ITX) PushFunc {
		if itx == COMMIT {
			return func(ircs Rcs) error {
				s.mem[v] = ircs
				return nil
			}
		}
		return nil
	}

	rcs, found = s.mem[v]

	return rcs, found, txFunc, nil
}
func (s memStorage) Find(v string) (rcs Rcs, found bool, err error) {
	rcs, found = s.mem[v]
	return rcs, found, nil
}

/*
func NewMxMemStorage() ILookuper {
	return &mxMemStorage {
		mem: make(mMap),
	}
}
func (s mxMemStorage) Store(v string) (a IAppender, txFunc TxFunc, err error) {
	txFunc = func(itx ITX) CommitFunc {
		if itx == COMMIT {
			return func(ia IAppender) error {
				s.m.Lock()
				s.mem[v] = ia
				s.m.Unlock()
				return nil
			}
		}
		return nil
	}
	var ok bool
	s.m.RLock()
	if a, ok = s.mem[v]; ok {
		s.m.RUnlock()
		return a, txFunc, nil
	}
	return a, txFunc, NotIndexed
}


func (s memStorage) Lookup(v string) (entries,error) {
	return s.mem[v], nil
}*/
