package srctype

import (
	"errors"
	// "gopkg.in/mgo.v2"
)

var EOF = errors.New("EOF")

//Reader load record from external source, Oracle, File, R
type Portaler interface {
	RemoteColNames() ([]string, error)
	RemoteRead() ([]string, error)
	RemoteReadAll() ([][]string, error)
	// RemoteReadWrite() error
	RemoteReadAllStr() (string, error)
	RemoteReadCh() chan interface{}
	Close()
}
