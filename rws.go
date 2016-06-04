package srctype

import (
	"github.com/lujiacn/rws"
)

type RwsSource struct {
	rawXml   []byte
	rawSlice []map[string]string
}

func NewRwsPortal(url, user, passwd string) (Portaler, error) {
	output, err := rws.RwsRead(url, user, passwd)
	if err != nil {
		return nil, err
	}
	rawSlice, err := rws.RwsToFlatMap(output)
	if err != nil {
		return nil, err
	}

	rwsPortal := &RwsSource{rawXml: output, rawSlice: rawSlice}
	return rwsPortal, nil

}

func (r *RwsSource) RemoteColNames() ([]string, error) {
	colNames := []string{}
	for k, _ := range r.rawSlice[0] {
		colNames = append(colNames, k)
	}
	return colNames, nil
}
func (r *RwsSource) RemoteRead() ([]string, error)      { return nil, nil }
func (r *RwsSource) RemoteReadAll() ([][]string, error) { return nil, nil }
func (r *RwsSource) RemoteReadAllStr() (string, error) {
	return string(r.rawXml), nil
}
func (r *RwsSource) RemoteReadCh() chan interface{} {
	resultC := make(chan interface{})
	go func() {
		for _, row := range r.rawSlice {
			resultC <- row
		}
		close(resultC)
	}()
	return resultC
}
func (r *RwsSource) Close() {}
