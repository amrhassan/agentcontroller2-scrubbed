package main

import (
	"fmt"
	"net/http"
	"github.com/gin-gonic/gin"
	"github.com/garyburd/redigo/redis"
	"encoding/json"
)

//
// data types
//
type CommandMessage struct {
	Id   string   `json:"id"`
	Gid  int      `json:"gid"`
	Nid  int      `json:"nid"`
	// Cmd  string   `json:"cmd"`
	// Args []string `json:"args"`
	// Data string   `json:"id"`
}

//
// redis stuff
//
func newPool() *redis.Pool {
	return &redis.Pool {
		MaxIdle: 80,
		MaxActive: 12000,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", ":6379")
			
			if err != nil {
				panic(err.Error())
			}
			
			return c, err
		},
	}
}

var pool = newPool()

//
// Command Reader
//
func cmdreader() {
	db := pool.Get()
        defer db.Close()
        
	for {
		//
		// waiting message from master queue
		//
		command, err := redis.Strings(db.Do("BLPOP", "__master__", "0"))
		
		fmt.Println("[+] message from master redis queue")
		
		if err != nil {
			fmt.Println("[-] pop error: ", err)
			continue
		}
		
		fmt.Println("[+] message payload: ", command[1])
		
		//
		// parsing json data
		//
		var payload CommandMessage
		err = json.Unmarshal([]byte(command[1]), &payload)
		
		if err != nil {
			fmt.Println("[-] message decoding: ", err)
			continue
		}
		
		id := fmt.Sprintf("%d:%d", payload.Gid, payload.Nid)
		fmt.Printf("[+] message destination [%s]\n", id)
		
		
		//
		// push message to client queue
		//
		_, err = db.Do("RPUSH", id, command[1])
		
		if err != nil {
			fmt.Println("[-] push error: ", err)
		}
	}
}

//
// REST stuff
//
func cmd(c *gin.Context) {
	gid := c.Param("gid")
	nid := c.Param("nid")
	
	fmt.Printf("[+] gin: execute (gid: %s, nid: %s)\n", gid, nid)
	
	//
	// New connection, checking this queue
	//
	db := pool.Get()
        defer db.Close()

	id := fmt.Sprintf("%s:%s", gid, nid)
	fmt.Printf("[+] waiting data from [%s]\n", id)
	
        pending, err := redis.Strings(db.Do("BLPOP", id, "0"))
        
        if err != nil {
		c.JSON(http.StatusInternalServerError, "error")
		return
	}
        
        //
        // extracting data from redis response
        //
        payload := pending[1]
        fmt.Printf("[+] payload: %s\n", payload)
	
	//
	// http reply
	//
	c.String(http.StatusOK, payload)
}

func log(c *gin.Context) {
	gid := c.Param("gid")
	nid := c.Param("nid")
	
	fmt.Printf("[+] gin: log (gid: %s, nid: %s)\n", gid, nid)
	
	// 
	
	
	//
	c.JSON(http.StatusOK, "ok")
}

func result(c *gin.Context) {
	gid := c.Param("gid")
	nid := c.Param("nid")
	
	fmt.Printf("[+] gin: result (gid: %s, nid: %s)\n", gid, nid)
	
	
	//
	c.JSON(http.StatusOK, "ok")
}

func stats(c *gin.Context) {
	gid := c.Param("gid")
	nid := c.Param("nid")
	
	fmt.Printf("[+] gin: stats (gid: %s, nid: %s)\n", gid, nid)
	
	
	//
	c.JSON(http.StatusOK, "ok")
}

func main() {	
	router := gin.Default()
	
	go cmdreader()
	
	router.GET("/:gid/:nid/cmd", cmd)
	router.GET("/:gid/:nid/log", log)
	router.GET("/:gid/:nid/result", result)
	router.GET("/:gid/:nid/stats", stats)
	
	
	router.Run(":8080")
}
