package master

import (
	"mtor/worker"
	"io/ioutil"
	"strings"
	"log"
	"fmt"
)


func operatorDDL(lines *[]string) (bool,error) {
	fmt.Printf("Start DDL operator ... \n")

	for _,line := range (*lines) {
		s := strings.Split(line," ")
		if len(s) >= 2 && s[0] == "DROP" && s[1] == "DATABASE" {
			dbName := s[4][0:len(s[4])-1]
			OK := worker.DeleteDB(dbName)
			if !OK {
        		log.Fatalf("delete db failed\n")
				return false, nil
			}
		}

		if len(s) >=2 && s[0] == "CREATE" && s[1] == "DATABASE" {
			dbName := s[5][0:len(s[5])-1]
			OK := worker.CreateDB(dbName)
			if !OK {
        		log.Fatalf("cant find a free db\n")
				return false, nil
			}
		}
	}

	for i,line := range (*lines) {
		// line := lines[i]
		s := strings.Split(line," ")
		if s[0] == "USE" {
			dbName := s[1][0:len(s[1])-1]
			worker.CreateTableinDB(dbName, lines, i + 1)
			continue 
		}
	}

	return true, nil
}

func operatorDML(lines *[]string) (bool,error) {
	fmt.Printf("Start DML operator ... \n")

	for _,line := range (*lines) {
		s := strings.Split(line," ")
		if len(s) >= 2 && s[0] == "source" {
			dumpName := s[1]
			OK := worker.InsertOneDump(dumpName)
			if !OK {
        		log.Fatalf("insert %s failed\n",dumpName)
				return false, nil
			}
		}
	}
	return true, nil
}

func Migrate(sqlFilePath string) (bool, error) {
	bytes, err := ioutil.ReadFile(sqlFilePath)
	if err != nil {
		return false, err
	}
	lines := strings.Split(string(bytes),"\n")

	OKDDL,errDDL := operatorDDL(&lines)
	if errDDL != nil {
		return OKDDL,errDDL
	}
	OKDML,errDML := operatorDML(&lines)
	if errDML != nil {
		return OKDML,errDML
	}
	
	return true, nil
}