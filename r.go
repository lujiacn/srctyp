package srctype

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lujiacn/rservcli"
	"gopkg.in/mgo.v2/bson"
	// "io/ioutil"
	"os"
	"reflect"
	"repal/app/lib"
)

//RSrouce for r file type
type RSource struct {
	rClient *rservcli.Rcli
	rScript string
}

//NewRPortal create instance for R script which return dataframe
func NewRPortal(host string, port int64, rScript string,
	argData bson.M, colMap map[string][]string,
	mongoDB string, mongoUrl string) (Portaler, error) {
	rClient, err := rservcli.NewRcli(host, port)
	if err != nil {
		return nil, err
	}
	r := &RSource{rClient: rClient, rScript: rScript}
	err = r.init(argData, colMap, mongoDB, mongoUrl)
	if err != nil {
		return r, err
	}

	return r, nil
}

func (r *RSource) argsAssign(srcQ bson.M, colMap map[string][]string) (err error) {
	if val, ok := srcQ["rmd_tpl"]; ok {
		err = r.rClient.Assign("rmd_tpl", val)
		if err != nil {
			return r.getErr()
		}
		delete(srcQ, "rmd_tpl")

	}
	for rVar, subQ := range srcQ {
		//assign colNames, and include review cols
		colNames := colMap[rVar]
		reviewCol := []string{"status", "action", "comments", "updated_at"}
		colNames = append(colNames, reviewCol...)
		r.rClient.Assign(fmt.Sprintf("%s_col", rVar), colNames)

		q, _ := json.Marshal(subQ)
		q_str := string(q)
		rScript := fmt.Sprintf(`
		tmp_dt <- extData$find('%s')
		if (is.null(tmp_dt$src_data)) {tmp_dt <- read.table(text="", col.names = %s_col)}
		if (!is.null(tmp_dt$src_data)) {
			if (!is.null(tmp_dt$review_record)) {
				tmp_dt <- data.frame(tmp_dt$src_data,tmp_dt$review_record)
			} else {
				tmp_dt <- data.frame(tmp_dt$src_data)
			}
		}
		%s <- tmp_dt
		`, q_str, rVar, rVar)

		//assign colMaps
		err = r.rClient.VoidEval(rScript)
		if err != nil {
			return r.getErr()
		}
	}
	return nil
}
func (r *RSource) getErr() error {
	var errMsg string
	errScript := `paste(repal_message_output, collapse="\n")`
	obj, _ := r.rClient.Eval(errScript)
	errMsg = fmt.Sprintf("%v", obj)
	if errMsg == "<nil>" {
		errMsg = "Error in r script"
	}
	return errors.New(errMsg)
}

func (r *RSource) init(argData bson.M, colMap map[string][]string, mongoDB string, mongoUrl string) (err error) {
	head_script := fmt.Sprintf(`
	t_con <- textConnection("repal_message_output", "w")
	sink(t_con, type="message")
	if(!require(mongolite)){
		install.packages("mongolite", repo="http://cran.us.r-project.org")
		library(mongolite)
	}
	extData <- mongo(collection="ExtData", db="%s", url="%s")
	`, mongoDB, mongoUrl)

	//start record r message
	r.rClient.VoidEval(head_script)

	//args Assignment
	err = r.argsAssign(argData, colMap)
	if err != nil {
		return err
	}

	rscript := r.rScript
	err = r.rClient.VoidEval(rscript)
	if err != nil {
		return r.getErr()
	}

	return err
}

//load for r, the output variable must be dataframe_output in r
func (r *RSource) dataframeIterator() error {
	//addtinal script for iterators
	script := `
	if (!require("iterators")) {
  		install.packages("iterators", repos='http://cran.us.r-project.org')
	}
	library("iterators")
	output_iter <- iter(dataframe_output, by='row')
	`
	err := r.rClient.VoidEval(script)
	if err != nil {
		return r.getErr()
	}
	return nil
}

//RemoteRead() for dataframe returned R script
func (r *RSource) RemoteRead() ([]string, error) {
	if r.rClient == nil {
		return nil, errors.New("No R connection")
	}

	out, err := r.rClient.Eval(`
    try(as.vector(unlist(nextElem(output_iter), use.names=FALSE)))
	`)
	if err != nil {
		return nil, err
	}
	switch out.(type) {
	case []string:
		return out.([]string), nil
	case string:
		return nil, EOF

	}
	return nil, nil
}

//RemoteReadAll() for dataframe returned R script
func (r *RSource) RemoteReadAll() ([][]string, error) {
	defer r.Close()

	if r.rClient == nil {
		return nil, errors.New("No R connection")
	}

	//addScript to received datafrom data as CSV string
	// addScript := `
	// txCon <- textConnection("csv_output", "w")
	// write.csv(dataframe_output, txCon, row.names=FALSE)
	// paste(csv_output, collapse="\n")
	// `
	//write to temp file and read file
	addScript := `
		if (exists("dataframe_output")) {
		  tempFileName <- tempfile()
		  write.csv(dataframe_output, tempFileName, row.names=FALSE)
		}
		if (exists("filename_output")) {
		  tempFileName <- filename_output
		}
		tempFileName
	`
	rawData, err := r.rClient.Eval(addScript)
	if err != nil {
		return nil, err
	}
	tempFileName := rawData.(string)
	//open tempfile
	f, err := os.Open(tempFileName)
	defer f.Close()
	defer os.Remove(tempFileName)

	if err != nil {
		return nil, errors.New("Error during open tmp file")
	}
	csvReader := csv.NewReader(bufio.NewReader(f))
	allData, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(allData) > 0 {
		return allData[1:len(allData)], nil
	}
	return allData, nil
}

//RemoColNames() for dataframe returned R script
func (r *RSource) RemoteColNames() ([]string, error) {
	if r.rClient == nil {
		return nil, errors.New("No R connection")
	}
	script := `
	col_names <- colnames(dataframe_output)
	`
	colNames, err := r.rClient.Eval(script)
	if err != nil {
		return nil, r.getErr()
	}
	switch colNames.(type) {
	case string:
		return []string{colNames.(string)}, nil
	case []string:
		return colNames.([]string), nil
	}
	return nil, errors.New("Unclear about col names.")
}

func (r *RSource) Close() {
	if r.rClient != nil {
		r.rClient.Close()
	}
}

//RemoteString will run R script and read string_output
func (r *RSource) RemoteReadAllStr() (string, error) {
	// addScript := `
	// if (exists("string_output")) {
	// tempFileName <- tempfile()
	// writeLines(string_output, tempFileName)
	// }
	// if (exists("filename_output")) {
	// tempFileName <- filename_output
	// }
	// tempFileName
	// `
	addScript := `paste(string_output, collapse="")`

	// r.rClient.VoidEval(addScript)
	rawData, err := r.rClient.Eval(addScript)
	if err != nil {
		// return "", errors.New("Error in r during write to file")
		return "", r.getErr()
	}
	fmt.Println(reflect.TypeOf(rawData))
	output := fmt.Sprintf("%v", rawData)
	// tempFileName := output
	// //Close R
	// r.Close()
	// //open tempfile
	// dat, err := ioutil.ReadFile(tempFileName)
	// if err != nil {
	// return "", errors.New("Error during reading r tmp file")
	// }
	// defer os.Remove(tempFileName)

	return string(output), err
}

func (r *RSource) RemoteReadCh() chan interface{} {
	resultC := make(chan interface{})

	//write to temp file and read file
	addScript := `
	tempFileName <- tempfile()
	write.csv(dataframe_output, tempFileName, row.names=FALSE)
	tempFileName
	`
	rawData, err := r.rClient.Eval(addScript)
	if err != nil {
		resultC <- err
		close(resultC)
		return resultC
	}
	tempFileName := rawData.(string)
	return lib.ReadCsvToArrayCh(tempFileName)
}
