package main

import (
	"container/list"
	"encoding/json"
	"flag"
	"fmt"
	hubbleAuth "github.com/Jumpscale/hubble/auth"
	hubble "github.com/Jumpscale/hubble/proxy"
	"github.com/garyburd/redigo/redis"
	"github.com/gin-gonic/gin"
	influxdb "github.com/influxdb/influxdb/client"
	"github.com/naoina/toml"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	AGENT_INACTIVER_AFTER_OVER = 10 * time.Minute
	ROLE_ALL                   = "*"
	COMMANDS_QUEUE             = "cmds_queue"
	RESULT_QUEUE               = "cmds_queue_%s"
	LOG_QUEUE                  = "joblog"
	JOBRESULT_HASH             = "jobresult:%s"
)

type Settings struct {
	Main struct {
		Listen        string
		RedisHost     string
		RedisPassword string
	}

	Influxdb struct {
		Host     string
		Db       string
		User     string
		Password string
	}

	Handlers struct {
		Binary string
		Cwd    string
		Env    map[string]string
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

// data types
type CommandMessage struct {
	Id     string `json:"id"`
	Gid    int    `json:"gid"`
	Nid    int    `json:"nid"`
	Role   string `json:"role"`
	Fanout bool   `json:"fanout"`
}

type CommandResult struct {
	Id        string `json:"id"`
	Nid       int    `json:"nid"`
	Gid       int    `json:"gid"`
	State     string `json:"state"`
	StartTime int64  `json:"starttime"`
}

type StatsRequest struct {
	Timestamp int64           `json:"timestamp"`
	Series    [][]interface{} `json:"series"`
}

type EvenRequest struct {
	Name string `json:"name"`
	Data string `json:"data"`
}

// redis stuff
func newPool(addr string, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:   80,
		MaxActive: 12000,
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialTimeout("tcp", addr, 0, 10*time.Minute, 0)

			if err != nil {
				panic(err.Error())
			}

			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}

			return c, err
		},
	}
}

var pool *redis.Pool

func isTimeout(err error) bool {
	return strings.Contains(err.Error(), "timeout")
}

func getAgentQueue(gid int, nid int) string {
	return fmt.Sprintf("cmds:%d:%d", gid, nid)
}

func getRoleQueue(role string) string {
	return fmt.Sprintf("cmds:%s", role)
}

func getGridRoleQueue(gid int, role string) string {
	return fmt.Sprintf("cmds:%d:%s", gid, role)
}

func getActiveAgents(onlyGid int) [][]int {
	producersLock.Lock()
	defer producersLock.Unlock()

	agents := make([][]int, 0, 10)
	for key, _ := range producers {
		var gid, nid int
		fmt.Sscanf(key, "%d:%d", &gid, &nid)
		if onlyGid > 0 && onlyGid != gid {
			continue
		}

		agents = append(agents, []int{gid, nid})
	}

	return agents
}

func readSingleCmd() bool {
	db := pool.Get()
	defer db.Close()

	command, err := redis.Strings(db.Do("BLPOP", COMMANDS_QUEUE, "0"))

	if err != nil {
		if isTimeout(err) {
			return true
		}

		log.Fatal("Coulnd't read new commands from redis", err)
	}

	log.Println("Received message:", command[1])

	// parsing json data
	var payload CommandMessage
	err = json.Unmarshal([]byte(command[1]), &payload)

	if err != nil {
		log.Println("message decoding error:", err)
		return true
	}

	//sort command to the consumer queue.
	//either by role or by the gid/nid.
	ids := list.New()

	if payload.Role != "" {
		//command has a given role
		if payload.Fanout {
			//fanning out.
			active := getActiveAgents(payload.Gid)
			for _, agent := range active {
				ids.PushBack(getAgentQueue(agent[0], agent[1]))
			}
		} else {
			if payload.Gid == 0 {
				//no specific Grid id
				ids.PushBack(getRoleQueue(payload.Role))
			} else {
				//no speicif Gid,
				ids.PushBack(getGridRoleQueue(payload.Gid, payload.Role))
			}
		}
	} else {
		ids.PushBack(getAgentQueue(payload.Gid, payload.Nid))
	}

	// push logs
	if _, err := db.Do("LPUSH", LOG_QUEUE, command[1]); err != nil {
		log.Println("[-] log push error: ", err)
	}

	for e := ids.Front(); e != nil; e = e.Next() {
		// push message to client queue
		log.Println("Dispatching message to", e.Value)
		if _, err := db.Do("RPUSH", e.Value.(string), command[1]); err != nil {
			log.Println("[-] push error: ", err)
		}
	}

	return true
}

// Command Reader
func cmdreader() {
	for {
		// waiting message from master queue
		if !readSingleCmd() {
			return
		}
	}
}

//Checks if x is in l
func In(l []string, x string) bool {
	for i := 0; i < len(l); i++ {
		if l[i] == x {
			return true
		}
	}

	return false
}

var producers map[string]chan *PollData = make(map[string]chan *PollData)
var producersLock sync.Mutex

/**
Gets a chain for the caller to wait on, we return a chan chan string instead
of chan string directly to make sure of the following:
1- The redis pop loop will not try to pop jobs out of the queue until there is a caller waiting
   for new commands
2- Prevent multiple clients polling on a single gid:nid at the same time.
*/
type PollData struct {
	Roles   []string
	MsgChan chan string
}

func getProducerChan(gid string, nid string) chan<- *PollData {
	key := fmt.Sprintf("%s:%s", gid, nid)

	producersLock.Lock()
	producer, ok := producers[key]
	if !ok {
		igid, _ := strconv.Atoi(gid)
		inid, _ := strconv.Atoi(nid)
		//start routine for this agent.
		log.Printf("Agent %s:%s active, starting agent routine\n", gid, nid)

		producer = make(chan *PollData)
		producers[key] = producer
		go func() {
			//db := pool.Get()

			defer func() {
				//db.Close()

				//no agent tried to connect
				close(producer)
				producersLock.Lock()
				defer producersLock.Unlock()
				delete(producers, key)
			}()

			for {
				if !func() bool {
					var data *PollData

					select {
					case data = <-producer:
					case <-time.After(AGENT_INACTIVER_AFTER_OVER):
						//no active agent for 10 min
						log.Printf("Agent", key, "is inactive for over 10 min, cleaning up.\n", gid, nid)
						return false
					}

					msgChan := data.MsgChan
					defer close(msgChan)

					roles := data.Roles
					roles_keys := make([]interface{}, 1, (len(roles)+1)*2+2)
					roles_keys[0] = getAgentQueue(igid, inid)

					for _, role := range roles {
						roles_keys = append(roles_keys,
							getRoleQueue(role),
							getGridRoleQueue(igid, role))
					}

					//add the ALL/ANY role queue.
					roles_keys = append(roles_keys,
						getRoleQueue(ROLE_ALL),
						getGridRoleQueue(igid, ROLE_ALL),
						"0")

					db := pool.Get()
					defer db.Close()

					pending, err := redis.Strings(db.Do("BLPOP", roles_keys...))
					if err != nil {
						if !isTimeout(err) {
							log.Println("Couldn't get new job for agent", key, err)
						}

						return true
					}

					// parsing json data
					var payload CommandMessage
					err = json.Unmarshal([]byte(pending[1]), &payload)

					//Note, by sending an empty command we are forcing the agent
					//to start a new poll immediately, it's better than leaving it to timeout if we just
					//returned true (continued)
					if err != nil {
						log.Println("Failed to load command", pending[1])
						pending[1] = ""
					}

					if payload.Role != "" && payload.Role != ROLE_ALL && !In(roles, payload.Role) {
						pending[1] = ""
					}

					select {
					case msgChan <- pending[1]:
						//caller consumed this job, it's safe to set it's state to RUNNING now.
						var payload CommandMessage
						if err := json.Unmarshal([]byte(pending[1]), &payload); err != nil {
							break
						}

						result_placehoder := CommandResult{
							Id:        payload.Id,
							Gid:       igid,
							Nid:       inid,
							State:     "RUNNING",
							StartTime: int64(time.Duration(time.Now().UnixNano()) / time.Millisecond),
						}

						if data, err := json.Marshal(&result_placehoder); err == nil {
							db.Do("HSET",
								fmt.Sprintf(JOBRESULT_HASH, payload.Id),
								key,
								data)
						}
					default:
						//caller didn't want to receive this command. have to repush it
						//directly on the agent queue. to avoid doing the redispatching.
						if pending[1] != "" {
							db.Do("LPUSH", getAgentQueue(igid, inid), pending[1])
						}
					}

					return true
				}() {
					return
				}
			}

		}()
	}
	producersLock.Unlock()

	return producer
}

// REST stuff
func cmd(c *gin.Context) {
	gid := c.Param("gid")
	nid := c.Param("nid")

	query := c.Request.URL.Query()
	roles := query["role"]
	log.Printf("[+] gin: execute (gid: %s, nid: %s)\n", gid, nid)

	// listen for http closing
	notify := c.Writer.(http.CloseNotifier).CloseNotify()

	timeout := 60 * time.Second

	producer := getProducerChan(gid, nid)

	data := &PollData{
		Roles:   roles,
		MsgChan: make(chan string),
	}

	select {
	case producer <- data:
	case <-time.After(timeout):
		c.String(http.StatusOK, "")
		return
	}
	//at this point we are sure this is the ONLY agent polling on /gid/nid/cmd

	var payload string

	select {
	case payload = <-data.MsgChan:
	case <-notify:
	case <-time.After(timeout):
	}

	c.String(http.StatusOK, payload)
}

func logs(c *gin.Context) {
	gid := c.Param("gid")
	nid := c.Param("nid")

	db := pool.Get()
	defer db.Close()

	log.Printf("[+] gin: log (gid: %s, nid: %s)\n", gid, nid)

	// read body
	content, err := ioutil.ReadAll(c.Request.Body)

	if err != nil {
		log.Println("[-] cannot read body:", err)
		c.JSON(http.StatusInternalServerError, "error")
		return
	}

	// push body to redis
	id := fmt.Sprintf("%s:%s:log", gid, nid)
	log.Printf("[+] message destination [%s]\n", id)

	// push message to client queue
	_, err = db.Do("RPUSH", id, content)

	c.JSON(http.StatusOK, "ok")
}

func result(c *gin.Context) {
	gid := c.Param("gid")
	nid := c.Param("nid")

	key := fmt.Sprintf("%s:%s", gid, nid)
	db := pool.Get()
	defer db.Close()

	log.Printf("[+] gin: result (gid: %s, nid: %s)\n", gid, nid)

	// read body
	content, err := ioutil.ReadAll(c.Request.Body)

	if err != nil {
		log.Println("[-] cannot read body:", err)
		c.JSON(http.StatusInternalServerError, "body error")
		return
	}

	// decode body
	var payload CommandMessage
	err = json.Unmarshal(content, &payload)

	if err != nil {
		log.Println("[-] cannot read json:", err)
		c.JSON(http.StatusInternalServerError, "json error")
		return
	}

	log.Println("Jobresult:", payload.Id)

	// update jobresult
	db.Do("HSET",
		fmt.Sprintf(JOBRESULT_HASH, payload.Id),
		key,
		content)

	// push message to client result queue queue
	db.Do("RPUSH", fmt.Sprintf(RESULT_QUEUE, payload.Id), content)

	c.JSON(http.StatusOK, "ok")
}

func stats(c *gin.Context) {
	gid := c.Param("gid")
	nid := c.Param("nid")

	log.Printf("[+] gin: stats (gid: %s, nid: %s)\n", gid, nid)

	// read body
	content, err := ioutil.ReadAll(c.Request.Body)

	if err != nil {
		log.Println("[-] cannot read body:", err)
		c.JSON(http.StatusInternalServerError, "body error")
		return
	}

	// decode body
	var payload []StatsRequest
	err = json.Unmarshal(content, &payload)

	if err != nil {
		log.Println("[-] cannot read json:", err)
		c.JSON(http.StatusInternalServerError, "json error")
		return
	}

	u, err := url.Parse(fmt.Sprintf("http://%s", settings.Influxdb.Host))
	if err != nil {
		log.Println(err)
		return
	}

	// building Influxdb requests
	con, err := influxdb.NewClient(influxdb.Config{
		Username: settings.Influxdb.User,
		Password: settings.Influxdb.Password,
		URL:      *u,
	})

	if err != nil {
		log.Println(err)
	}

	points := make([]influxdb.Point, 0, 100)

	for _, stats := range payload {
		for i := 0; i < len(stats.Series); i++ {
			point := influxdb.Point{
				Measurement: stats.Series[i][0].(string),
				Time:        time.Unix(stats.Timestamp, 0),
				Fields: map[string]interface{}{
					"value": stats.Series[i][1],
				},
			}

			points = append(points, point)
		}
	}

	batchPoints := influxdb.BatchPoints{
		Points:          points,
		Database:        settings.Influxdb.Db,
		RetentionPolicy: "default",
	}

	if _, err := con.Write(batchPoints); err != nil {
		log.Println(err)
		return
	}

	c.JSON(http.StatusOK, "ok")
}

func event(c *gin.Context) {
	gid := c.Param("gid")
	nid := c.Param("nid")

	log.Printf("[+] gin: event (gid: %s, nid: %s)\n", gid, nid)

	content, err := ioutil.ReadAll(c.Request.Body)

	if err != nil {
		log.Println("[-] cannot read body:", err)
		c.JSON(http.StatusInternalServerError, "body error")
		return
	}

	var payload EvenRequest
	log.Printf("%s", content)
	err = json.Unmarshal(content, &payload)
	if err != nil {
		log.Println(err)
		c.JSON(http.StatusInternalServerError, "Error")
	}

	cmd := exec.Command(settings.Handlers.Binary,
		fmt.Sprintf("%s.py", payload.Name), gid, nid)

	cmd.Dir = settings.Handlers.Cwd
	//build env string
	var env []string
	if len(settings.Handlers.Env) > 0 {
		env = make([]string, 0, len(settings.Handlers.Env))
		for ek, ev := range settings.Handlers.Env {
			env = append(env, fmt.Sprintf("%v=%v", ek, ev))
		}

	}

	cmd.Env = env

	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Println("Failed to open process stderr", err)
	}

	log.Println("Starting handler for", payload.Name, "event, for agent", gid, nid)
	err = cmd.Start()
	if err != nil {
		log.Println(err)
	} else {
		go func() {
			//wait for command to exit.
			cmderrors, err := ioutil.ReadAll(stderr)
			if len(cmderrors) > 0 {
				log.Printf("%s(%s:%s): %s", payload.Name, gid, nid, cmderrors)
			}

			err = cmd.Wait()
			if err != nil {
				log.Println("Failed to handle ", payload.Name, " event for agent: ", gid, nid, err)
			}
		}()
	}

	c.JSON(http.StatusOK, "ok")
}

func hubbleProxy(context *gin.Context) {
	hubble.ProxyHandler(context.Writer, context.Request)
}

var settings Settings

func main() {
	var cfg string
	var help bool

	flag.BoolVar(&help, "h", false, "Print this help screen")
	flag.StringVar(&cfg, "c", "", "Path to config file")
	flag.Parse()

	printHelp := func() {
		log.Println("agentcontroller [options]")
		flag.PrintDefaults()
	}

	if help {
		printHelp()
		return
	}

	if cfg == "" {
		log.Println("Missing required option -c")
		flag.PrintDefaults()
		os.Exit(1)
	}

	LoadTomlFile(cfg, &settings)

	log.Printf("[+] webservice: <%s>\n", settings.Main.Listen)
	log.Printf("[+] redis server: <%s>\n", settings.Main.RedisHost)

	pool = newPool(settings.Main.RedisHost, settings.Main.RedisPassword)

	db := pool.Get()
	if _, err := db.Do("PING"); err != nil {
		panic(fmt.Sprintf("Failed to connect to redis: %v", err))
	}

	db.Close()

	router := gin.Default()

	go cmdreader()

	hubbleAuth.Install(hubbleAuth.NewAcceptAllModule())
	router.GET("/:gid/:nid/cmd", cmd)
	router.POST("/:gid/:nid/log", logs)
	router.POST("/:gid/:nid/result", result)
	router.POST("/:gid/:nid/stats", stats)
	router.POST("/:gid/:nid/event", event)
	router.GET("/:gid/:nid/hubble", hubbleProxy)
	// router.Static("/doc", "./doc")

	router.Run(settings.Main.Listen)
}
