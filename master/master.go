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

// func operatorDML(lines *[]string) (bool,error) {
// 	fmt.Printf("Start DML operator ... \n")

// 	for _,line := range (*lines) {
// 		s := strings.Split(line," ")
// 		if len(s) >= 2 && s[0] == "source" {
// 			dumpName := s[1]
// 			OK,_ := worker.InsertOneDump(dumpName)
// 			if !OK {
//         		log.Fatalf("insert %s failed\n",dumpName)
// 				return false, nil
// 			}
// 		}
// 	}
// 	return true, nil
// }

func operatorDML(lines *[]string) (bool,error) {
	fmt.Printf("Start DML operator ... \n")

	dumpSum := 0
	// dumpNames := []string{}
	cc := make(chan string, 0)

	for _,line := range (*lines) {
		s := strings.Split(line," ")
		if len(s) >= 2 && s[0] == "source" {
			dn := s[1]
			go func(dumpName string, c chan string) {
				_,dnBack := worker.InsertOneDump(dumpName)
				c <- dnBack
			} (dn,cc)

			dumpSum += 1
			// dumpNames = append(dumpNames, dn)
		}
	}

	flag := true
	for i := 0; i < dumpSum; i ++ {
		errorName := <- cc
		fmt.Printf("(%v)\n",errorName)
		if errorName != "" {
			fmt.Printf("dump %v failed\n",errorName)
			flag = false
		}
	}
	return flag, nil
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