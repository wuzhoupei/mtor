package main

import (
	"mtor/master"
	"mtor/CMD"
	"mtor/worker"
	"os"
	"log"
	"fmt"
	"time"
	"strconv"
)

func main() {
	f, errf := os.OpenFile("./time_ruselt.txt", os.O_CREATE | os.O_APPEND | os.O_WRONLY, 0200)
	if errf != nil {
		fmt.Println(errf)
		return 
	}
	
	defer f.Close()

	startTime := time.Now()
	fmt.Printf("Now is %v\n", startTime)
	_, errt := f.WriteString(startTime.Format("2006-01-02 15:04:05\n"))
	if errt != nil {
		fmt.Println(errt)
		return
	}

	CMD.LinkRedis()
	defer CMD.CloseRedis()

	var sqlFilePath string = "/root/mtor/test_db/sample.sql"
	worker.DumpPath = "/root/mtor/test_db/"
	OK, err := master.Migrate(sqlFilePath)

	if !OK {
        log.Fatalf("Migrate data from Mysql to redis error : %s\n", err)
	} else {
        fmt.Printf("Migrate data from Mysql to redis Accepted !\n")
	}
	
	endTime := time.Now()
	fmt.Printf("Now is %v\n", endTime)
	fmt.Printf("Use time : %v\n",endTime.Sub(startTime))
	
	_, errt = f.WriteString(endTime.Format("2006-01-02 15:04:05")+"\n")
	_, errt = f.WriteString(strconv.FormatFloat(endTime.Sub(startTime).Seconds(), 'f', 2, 64)+"\n")

	// key := "t1\x00r1"
	// value,err1 := CMD.RedisGet(key)
	// fmt.Printf("(%v),(%v)\n",value,err1)
	// var a,b string = string('\x00'), "aaa"
	// var c []byte
	// c = append(c,(b+a+b)...)
	// d := string(c)
	// fmt.Printf("%v\n%s\n",c,d)
}