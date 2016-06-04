package srctype

import (
	// "encoding/csv"
	"errors"
	// "gopkg.in/mgo.v2"
	"io/ioutil"
	// "os"
	"repal/app/lib"
	"strings"
)

type FileSource struct {
	// adapter    *Adapter
	// mgoSession *mgo.Session
	fileName   string
	colNames   []string
	rawData    [][]string
	readChFunc func(string) chan interface{}
}

//New CSVFilePortal
func NewCSVPortal(fileName string) (Portaler, error) {
	o := &FileSource{fileName: fileName}
	if err := o.load(fileName); err != nil {
		return nil, err
	}
	return o, nil
}

//NewFlatPortal reading rawdata and return string
func NewFlatPortal(fileName string) (Portaler, error) {
	o := &FileSource{fileName: fileName}
	return o, nil
}

//RemoteString
func (c *FileSource) RemoteString() (string, error) {
	dat, err := ioutil.ReadFile(c.fileName)
	if err != nil {
		return "", err
	}
	return string(dat), nil
}

//load function as initiate function for new interface
func (c *FileSource) load(fileName string) (err error) {
	//open File
	var fileExt string

	fileExtList := strings.Split(fileName, ".")
	if len(fileExtList) == 1 {
		fileExt = "csv"
	} else {
		fileExt = fileExtList[len(fileExtList)-1]
	}
	switch strings.ToLower(fileExt) {
	case "csv":
		c.colNames, err = lib.ReadCsvColNames(fileName)
		c.readChFunc = lib.ReadCsvToArrayCh
		if err != nil {
			return err
		}
	case "xls", "xlsx":
		c.colNames, err = lib.ReadXlsColNames(fileName)
		c.readChFunc = lib.ReadXlsToArrayCh
		if err != nil {
			return err
		}
	default:
		err := errors.New("Unsupported file type!")
		return err
	}

	return nil
}

//must before Read()
func (c *FileSource) RemoteColNames() ([]string, error) {
	return lib.ColNameReplace(c.colNames), nil
}

func (c *FileSource) RemoteRead() ([]string, error) {
	return nil, errors.New("Method not implemented")
}

//do not include 1st row (header)
func (c *FileSource) RemoteReadAll() ([][]string, error) {
	return c.rawData, nil
}

func (c *FileSource) Close() {
}

func (c *FileSource) RemoteReadAllStr() (string, error) {
	//open tempfile
	dat, err := ioutil.ReadFile(c.fileName)
	if err != nil {
		return "", errors.New("Error during reading file")
	}

	return string(dat), err
}

func (c *FileSource) RemoteReadCh() chan interface{} {
	return c.readChFunc(c.fileName)
}
