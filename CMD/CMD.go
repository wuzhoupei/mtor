package CMD

import (
	"github.com/garyburd/redigo/redis"
	"log"
	"fmt"
	"strconv"
)

var client redis.Conn

func LinkRedis() {
	var err error
	client, err = redis.Dial("tcp", "localhost:6379")
	fmt.Printf("link error : %v\n",err)

	if err != nil {
        log.Fatalln(err)
    }
}

func PingRedis() (string, error) {
	pong, err := redis.String(client.Do("ping"))
	return pong, err
}

func RedisSet(k,v string) error {
	_,err := client.Do("set", k, v)
	return err
}

func RedisGet(k string) (string, error) {
	x,err := redis.String(client.Do("get", k))
	return x,err
}

func RedisDel(k string) error {
	_, err := client.Do("del", k)
	return err
}

func ChangeDB(x int) {
	client.Do("select", strconv.Itoa(x))
}

func ChangeDBbyName(name string) {
	dbSum := 16
	for i := 0; i <= dbSum; i ++ {
		ChangeDB(i)
		dbName, _ := RedisGet(name)
		if dbName == name {
			return 
		}
	}
}

func FlushDB() {
	client.Do("flushdb")
}

func CloseRedis() {
	client.Close()
}