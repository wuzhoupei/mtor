package worker

import (
	"mtor/CMD"
	"mtor/codec"
	"fmt"
	"strings"

	// "github.com/go-redis/redis"
)

// ---------- Database 

const DB_MAX_ID = 16

func ChangeDB(x int) {
	fmt.Printf("Change db by id \"%d\"\n",x)
	CMD.ChangeDB(x)
}

func ChangeDBto(dbName string) {
	fmt.Printf("Change db by name \"%s\"\n",dbName)
	for i := 0; i < DB_MAX_ID; i ++ {
		ChangeDB(i)

		v, _ := CMD.RedisGet("_DBName")

		if v == dbName {
			return 
		}
	}
}

func CreateDB(dbName string) bool {
	fmt.Printf("create %s\n",dbName)
	for i := 0; i < DB_MAX_ID; i ++ {
		ChangeDB(i)

		v, err := CMD.RedisGet("_DBName")
		fmt.Printf("db %d : %v  %v  \n",i, len(v), err)
		
		if v == dbName {
			return true
		}

		if v == "" {
			err = CMD.RedisSet("_DBName", dbName)
			if err == nil {
				return true
			}
		}
	}

	return false
}

func DeleteDB(dbName string) bool {
	fmt.Printf("delete %s\n",dbName)
	flag := true
	for i := 0; i < DB_MAX_ID; i ++ {
		ChangeDB(i)

		v, _ := CMD.RedisGet("_DBName")
		fmt.Printf("db %d : %v  \n",i, v)

		if v == dbName {
			CMD.FlushDB()
			return flag
		}
	}

	return flag
}

// ---------- Table 

/*
* get the name which need be del, and return by a []sting
*/
func splitTableName(lines *[]string, i int) ([]string, int) {
	newi := i
	lineLen := len(*lines)
	var tn []string
	for ; newi < lineLen; newi ++ {
		line := strings.Fields((*lines)[newi])
		if len(line) < 1 {
			continue 
		}

		tn = append(tn,line[len(line)-1])
		
		if tn[newi-i][len(tn[newi-i])-1] == ';' {
			tn[newi-i] = tn[newi-i][0:len(tn[newi-i])-1]
			return tn,newi
		} else {
			tn[newi-i] = tn[newi-i][0:len(tn[newi-i])-1]
		}
	}
	return nil,newi
}

/*
* del the table by name.
* acturly, this function need del the all key in this table.
* but this is a new db, it is empty at start so that we dont need do that.
* other way, this is a error function.
*/
func delTable(dbName,tn string) {
	key := string(codec.Encode_tbn(tn))
	_, err := CMD.RedisGet(key)
	if err != nil {
		return 
	}

	err = CMD.RedisDel(key) // 要删除表中所有的值，此处暂不做考虑
	if err == nil {
		return 
	}
}

/*
* create a table by name, and we need Set the kv twice,
* just for make us can get a table by id and name.
*/
func creTable(dbName,tn string) int {
	key := string(codec.Encode_tbn(tn))
	tbi, err := CMD.RedisGet(key)
	if err == nil {
		return codec.Decode_tbi(tbi)
	}

	for i := 0; ; i++ {
		key = string(codec.Encode_tbi(i))
		_, err := CMD.RedisGet(key)
		if err != nil {
			value := string(codec.Encode_tbn(tn))
			err = CMD.RedisSet(key,value)
			err = CMD.RedisSet(value,key)
			return i
		}
	} 
}

/*
* in No.ti table, we create a column by name type and if it is NULL,
* we Set it twice, too.
*/
func creOneColumn(ti int, cn,ct string, cnull bool) {
	var key, value string
	for i := 0; ; i ++ {
		key = string(codec.Encode_tbi_ci(ti,i))
		value = string(codec.Encode_cc_ct_cn(cnull,ct,cn))
		_, err := CMD.RedisGet(key)
		if err != nil {
			err = CMD.RedisSet(key,value)

			key = string(codec.Encode_tbi_cn(ti,cn))
			value = string(codec.Encode_cc_ct_ci(cnull,ct,i))
			err = CMD.RedisSet(key,value)
			return 
		}
	}
}

/*
* find all line which every one include a column order,
* and create them one by one.
*/
func creColumnsOnTable(lines *[]string, i,ti,lineLen int) {
	var columnName, columnType string
	var canNULL, needRet bool = false, false
	for ; i < lineLen ; i ++ {
		line := strings.Fields((*lines)[i])
		if len(line) < 2 {
			continue 
		}

		columnName = line[0]
		columnType = line[1]
		if line[len(line)-1][len(line[len(line)-1])-1] != ',' {
			needRet = true
		} else {
			line[len(line)-1] = line[len(line)-1][0:len(line[len(line)-1])-2]
		}

		
		fmt.Printf("%v\n",line)

		if line[len(line)-1] == "NULL" {
			canNULL = false
		} else {
			canNULL = true
		}

		creOneColumn(ti, columnName, columnType, canNULL)

		if needRet {
			return
		}
	}
}

/*
* create a db, include : db, each table in db, each column in each table 
*/
func CreateTableinDB(dbName string, lines *[]string, i int) {
	// change the db to need
	ChangeDBto(dbName)

	var lineLen int = len(*lines)
	fmt.Printf("lineLen : %v and i from %v\n",lineLen, i)

	for ; i < lineLen; i ++ {
		line := (*lines)[i]
		s := strings.Fields(line)
		fmt.Printf("line : (%T) %v %v\n",s, len(s),s)
		if len(s) <= 0 {
			continue 
		}

		// this job just do one db
		if s[0] == "USE" {
			return 
		}

		// find the del order
		if len(s) >= 2 && s[0] == "DROP" && s[1] == "TABLE" {
			// get the db need del by a []sting
			tableName, newi := splitTableName(lines, i)
			fmt.Printf("%v\n",tableName)
			for _, tn := range tableName {
				// del the table by name in the db
				delTable(dbName, tn)
			}

			// goto next DDL order
			i = newi
			continue 
		}

		// find the creat table order
		if len(s) >=2 && s[0] == "CREATE" && s[1] == "TABLE" {
			tableName := s[2][0:len(s[2])]
			fmt.Printf("table name will be created : %v\n",tableName)
			// create a table by name, and get this new table's id
			tableid := creTable(dbName, tableName)
			// create each column in this table
			creColumnsOnTable(lines, i + 1, tableid, lineLen)
		}
	}
}