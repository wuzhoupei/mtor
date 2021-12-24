package CMD

import (
	"github.com/garyburd/redigo/redis"
	"log"
	"fmt"
	"sync"
	"strconv"
)

var client redis.Conn
var mutex sync.Mutex

func LinkRedis() {
	mutex.Lock()
	defer mutex.Unlock()
	var err error
	client, err = redis.Dial("tcp", "localhost:6379")
	fmt.Printf("link error : %v\n",err)

	if err != nil {
        log.Fatalln(err)
    }
}

func PingRedis() (string, error) {
	mutex.Lock()
	defer mutex.Unlock()
	pong, err := redis.String(client.Do("ping"))
	return pong, err
}

func RedisSet(k,v string) error {
	mutex.Lock()
	defer mutex.Unlock()
	_,err := client.Do("set", k, v)
	return err
}

func RedisGet(k string) (string, error) {
	mutex.Lock()
	defer mutex.Unlock()
	x,err := redis.String(client.Do("get", k))
	return x,err
}

func RedisDel(k string) error {
	mutex.Lock()
	defer mutex.Unlock()
	_, err := client.Do("del", k)
	return err
}

func ChangeDB(x int) {
	mutex.Lock()
	defer mutex.Unlock()
	client.Do("select", strconv.Itoa(x))
}

func ChangeDBbyName(name string) {
	mutex.Lock()
	defer mutex.Unlock()
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
	mutex.Lock()
	defer mutex.Unlock()
	client.Do("flushdb")
}

func CloseRedis() {
	mutex.Lock()
	defer mutex.Unlock()
	client.Close()
}