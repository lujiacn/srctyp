package srctype

import (
	"database/sql"
	"errors"
	// "fmt"
	"github.com/lujiacn/sqlutils"
	"repal/app/lib"
	// "strings"
)

type SqlSource struct {
	db        *sql.DB
	rows      *sql.Rows
	sqlScript string
}

func NewSqlPortal(db *sql.DB, sqlScript string) (Portaler, error) {
	o := new(SqlSource)
	o.db = db
	o.sqlScript = sqlScript
	if err := o.load(); err != nil {
		return nil, err
	}

	return o, nil
}

//load is oracle db initiation
func (o *SqlSource) load() error {
	// sql = strings.Replace(sql, "&&study", o.adapter.Study, -1)
	sqlScript := o.sqlScript
	rows, err := o.db.Query(sqlScript)
	if err != nil {
		return err
	}
	o.rows = rows
	return nil
}

//ColNames get record column names
func (o *SqlSource) RemoteColNames() (colNames []string, err error) {
	colNames, err = o.rows.Columns()
	if err != nil {
		return nil, err

	}
	return lib.ColNameReplace(colNames), nil
}

//Read is iterator, return one row each time, cannot be used with ReadAll() together
func (o *SqlSource) RemoteRead() ([]string, error) {
	return nil, errors.New("function not implemented")
}

//ReadAll read all result and close the row at end. Can not be used with Read() together
func (o *SqlSource) RemoteReadAll() (output [][]string, err error) {
	defer o.rows.Close()
	if o.rows == nil {
		return nil, errors.New("No connection.")
	}

	output, err = sqlutils.RowToArr(o.rows)
	if err != nil {
		return nil, err
	}
	return output[1:len(output)], nil

}

//Close function
func (o *SqlSource) Close() {
	if o.rows != nil {
		o.rows.Close()
	}
}

func (o *SqlSource) RemoteReadAllStr() (string, error) {
	return "Sql Data", nil
}

func (o *SqlSource) RemoteReadCh() chan interface{} {
	return sqlutils.RowToArrayChan(o.rows)
}
