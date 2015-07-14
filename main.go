package main

import (
	"os"
	"fmt"
	"flag"
	"net/http"
	"io/ioutil"
	"github.com/gin-gonic/gin"
	"github.com/garyburd/redigo/redis"
	"github.com/naoina/toml"
	"encoding/json"
	"github.com/Jumpscale/jsagentcontroller/influxdb-client-0.8.8"
)

type Settings struct {
	Main struct {
		Listen string
		Redis string
	}

	Influxdb struct {
		Host string
		Db string
		User string
		Password string
	}
}

//LoadTomlFile loads toml using "github.com/naoina/toml"
func LoadTomlFile(filename string, v interface{}) {
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	buf, err := ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}
	if err := toml.Unmarshal(buf, v); err != nil {
		panic(err)
	}
}

//
// data types
//
type CommandMessage struct {
	Id   string  `json:"id"`
	Gid  int     `json:"gid"`
	Nid  int     `json:"nid"`
}

type StatsRequest struct {
	Timestamp int             `json:"timestamp"`
	Series    [][]interface{} `json:"series"`
}

//
// redis stuff
//
func newPool(addr string) *redis.Pool {
	return &redis.Pool {
		MaxIdle: 80,
		MaxActive: 12000,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", addr)

			if err != nil {
				panic(err.Error())
			}

			return c, err
		},
	}
}

var pool *redis.Pool

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
		command, err := redis.Strings(db.Do("BLPOP", "cmds_queue", "0"))

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

func logs(c *gin.Context) {
	gid := c.Param("gid")
	nid := c.Param("nid")

	db := pool.Get()
		defer db.Close()

	fmt.Printf("[+] gin: log (gid: %s, nid: %s)\n", gid, nid)

	//
	// read body
	//
	content, err := ioutil.ReadAll(c.Request.Body)

	if err != nil {
		fmt.Println("[-] cannot read body:", err)
		c.JSON(http.StatusInternalServerError, "error")
		return
	}

	//
	// push body to redis
	//
	id := fmt.Sprintf("%s:%s:log", gid, nid)
	fmt.Printf("[+] message destination [%s]\n", id)

	//
	// push message to client queue
	//
	_, err = db.Do("RPUSH", id, content)

	//
	c.JSON(http.StatusOK, "ok")
}

func result(c *gin.Context) {
	gid := c.Param("gid")
	nid := c.Param("nid")

	db := pool.Get()
		defer db.Close()

	fmt.Printf("[+] gin: result (gid: %s, nid: %s)\n", gid, nid)

	//
	// read body
	//
	content, err := ioutil.ReadAll(c.Request.Body)

	if err != nil {
		fmt.Println("[-] cannot read body:", err)
		c.JSON(http.StatusInternalServerError, "body error")
		return
	}

	//
	// decode body
	//
	var payload CommandMessage
	err = json.Unmarshal(content, &payload)

	if err != nil {
		fmt.Println("[-] cannot read json:", err)
		c.JSON(http.StatusInternalServerError, "json error")
		return
	}

	fmt.Printf("[+] payload: jobid: %d\n", payload.Id)

	//
	// push body to redis
	//
	fmt.Printf("[+] message destination [%s]\n", payload.Id)


	//
	// push message to client queue
	//
	_, err = db.Do("RPUSH", fmt.Sprintf("cmds_queue_%s", payload.Id), content)

	//
	c.JSON(http.StatusOK, "ok")
}

func stats(c *gin.Context) {
	gid := c.Param("gid")
	nid := c.Param("nid")

	fmt.Printf("[+] gin: stats (gid: %s, nid: %s)\n", gid, nid)

	//
	// read body
	//
	content, err := ioutil.ReadAll(c.Request.Body)

	if err != nil {
		fmt.Println("[-] cannot read body:", err)
		c.JSON(http.StatusInternalServerError, "body error")
		return
	}

	//
	// decode body
	//
	var payload StatsRequest
	err = json.Unmarshal(content, &payload)

	if err != nil {
		fmt.Println("[-] cannot read json:", err)
		c.JSON(http.StatusInternalServerError, "json error")
		return
	}

	//
	// building Influxdb requests
	//
	con, err := client.NewClient(&client.ClientConfig{
		Username: settings.Influxdb.User,
		Password: settings.Influxdb.Password,
		Database: settings.Influxdb.Db,
		Host:     settings.Influxdb.Host,
	})

	if err != nil {
		fmt.Println(err)
	}

	var timestamp = payload.Timestamp

	for i := 0; i < len(payload.Series); i++ {
		series := &client.Series{
			Name: "test",
			Columns: []string{"gid", "nid", "time", "key", "value"},
			// FIXME: add all points then write once
			Points: [][]interface{} {{
				gid,
				nid,
				int64(timestamp),
				payload.Series[i][0],
				payload.Series[i][1],
			},},
		}

		if err := con.WriteSeries([]*client.Series{series}); err != nil {
			fmt.Println(err)
			return
		}
	}

	//
	c.JSON(http.StatusOK, "ok")
}

var settings Settings

func main() {
	var cfg string
	var help bool

	flag.BoolVar(&help, "h", false, "Print this help screen")
	flag.StringVar(&cfg, "c", "", "Path to config file")
	flag.Parse()

	printHelp := func() {
		fmt.Println("agentcontroller [options]")
		flag.PrintDefaults()
	}

	if help {
		printHelp()
		return
	}

	if cfg == "" {
		fmt.Println("Missing required option -c")
		flag.PrintDefaults()
		os.Exit(1)
	}

	LoadTomlFile(cfg, &settings)

	fmt.Printf("[+] webservice: <%s>\n", settings.Main.Listen)
	fmt.Printf("[+] redis server: <%s>\n", settings.Main.Redis)

	pool = newPool(settings.Main.Redis)
	router := gin.Default()

	go cmdreader()

	router.GET("/:gid/:nid/cmd", cmd)
	router.POST("/:gid/:nid/log", logs)
	router.POST("/:gid/:nid/result", result)
	router.POST("/:gid/:nid/stats", stats)
	// router.Static("/doc", "./doc")

	router.Run(settings.Main.Listen)
}
