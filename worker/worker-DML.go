package worker

import (
	"os"
	"fmt"
	"bufio"
	"strings"
	"mtor/CMD"
	"mtor/codec"
)

var DumpPath string
// var tableName string
// var columnType []int
// var t,rowId int

func getTableName(line *string) string {
	for i,c := range (*line) {
		if c == '`' {
			endi := i + 1
			for (*line)[endi] != '`' {
				endi += 1
			}
			return (*line)[i+1:endi]
		}
	}

	return ""
}

func getRowValue(line *string) string {
	for i,c := range (*line) {
		if c == '(' {
			return (*line)[i:len(*line)]
		}
	}

	return ""
}

func getColumnsType(t int) []int {
	columnType := []int{}
	for i := 0; ; i ++ {
		cType,_ := codec.Decode_ti_ci_Type(t, i)
		
		if len(cType) == 0 {
			// fmt.Printf("%d , %v\n",t,columnType)
			return columnType
		}

		switch cType[0] {
		case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT":
			columnType = append(columnType, 0)
			break

		case "FLOAT", "DOUBLE":
			columnType = append(columnType, 1)
			break

		case "CHAR", "VARCHAR", "TINYBLOB", "TINYTEXT", "BLOB", "TEXT", "MEDIUMBLOB", "MEDIUMTEXT", "LONGBLOB", "LONGTEXT":
			columnType = append(columnType, 2)
			break 

		case "DATE":
			columnType = append(columnType, 3)
			break 

		case "TIME":
			columnType = append(columnType, 4)
			break 

		case "YEAR":
			columnType = append(columnType, 5)
			break 

		case "DATETIME":
			columnType = append(columnType, 6)
			break 

		case "TIMESTAMP":
			columnType = append(columnType, 7)
			break 
		}
	} 
}

func insertOneLine(line string, t int, rowId *int, cTarr *[]int) {
	line = line[1:len(line)-2]
	s := strings.Split(line, ",")
	var keyr,valuer string
	var keyc,valuec string

	keyr = string(codec.Encode_t_r(t,*rowId))

	for i,x := range s {
		if x[0] == '\'' {
			x = x[1:len(x)-1]
		}
		valuer += string(codec.Encode_Pt_Pe((*cTarr)[i], x))
		keyc = string(codec.Encode_t_ci_ce_r(t, i, x, *rowId))
		CMD.RedisSet(keyc, valuec)
	}

	CMD.RedisSet(keyr, valuer)

	*rowId += 1
}

func InsertOneDump(dumpName string) (bool,string) {
	rowId := 0
	columnType := []int{}
	tableName := ""
	t := 0

	f,err := os.Open(DumpPath + dumpName)
	if err != nil {
		return false,dumpName
	}

	fmt.Printf("Start %v\n",dumpName)

	buf := bufio.NewScanner(f)
	for buf.Scan() {
		line := buf.Text()
		if len(line) <= 0 {
			continue
		}

		if len(line) >= 6 && line[0:6] == "INSERT" {
			tableName = getTableName(&line)
			t = codec.Decode_tn_Id(tableName)
			columnType = getColumnsType(t)
			line = getRowValue(&line)
		}
		if len(line) > 0 {
			insertOneLine(line, t, &rowId, &columnType)
		}
	}

	fmt.Printf("%d\n",rowId)

	return true,""
}