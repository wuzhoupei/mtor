package main

import (
	"mtor/master"
	"mtor/CMD"
	"mtor/worker"
	"log"
	"fmt"
)

func main() {
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

	key := "t1\x00r1"
	value,err1 := CMD.RedisGet(key)
	fmt.Printf("(%v),(%v)\n",value,err1)
	var a,b string = string('\x00'), "aaa"
	var c []byte
	c = append(c,(b+a+b)...)
	d := string(c)
	fmt.Printf("%v\n%s\n",c,d)
}