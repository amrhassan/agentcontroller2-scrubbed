package main

import (
	"container/list"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	hublleAgent "github.com/Jumpscale/hubble/agent"
	hubbleAuth "github.com/Jumpscale/hubble/auth"
	hublleProxy "github.com/Jumpscale/hubble/proxy"
	"github.com/garyburd/redigo/redis"
	"github.com/gin-gonic/gin"
	influxdb "github.com/influxdb/influxdb/client"
)

const (
	AGENT_INACTIVER_AFTER_OVER = 30 * time.Second
	ROLE_ALL                   = "*"
	COMMANDS_QUEUE             = "cmds.queue"
	JOBS_QUEUED_QUEUE          = "cmd.%s.queued"
	AGENT_RESULT_QUEUE         = "cmd.%s.%d.%d"
	LOG_QUEUE                  = "joblog"
	JOBRESULT_HASH             = "jobresult:%s"
	INTERNAL_COMMAND           = "controller"
)

var TAGS = []string{"gid", "nid", "command", "domain", "name", "measurement"}

// data types
type CommandMessage struct {
	Id     string   `json:"id"`
	Gid    int      `json:"gid"`
	Nid    int      `json:"nid"`
	Cmd    string   `json:"cmd"`
	Roles  []string `json:"roles"`
	Fanout bool     `json:"fanout"`
	Args   struct {
		Name string `json:"name"`
	} `json:"args"`
}

type CommandResult struct {
	Id        string `json:"id"`
	Nid       int    `json:"nid"`
	Gid       int    `json:"gid"`
	State     string `json:"state"`
	Data      string `json:"data"`
	Level     int    `json:"level"`
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
			c, err := redis.DialTimeout("tcp", addr, 0, AGENT_INACTIVER_AFTER_OVER/2, 0)

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

func getAgentResultQueue(result *CommandResult) string {
	return fmt.Sprintf(AGENT_RESULT_QUEUE, result.Id, result.Gid, result.Nid)
}

func getActiveAgents(onlyGid int, roles []string) [][]int {
	producersLock.Lock()
	defer producersLock.Unlock()

	checkRole := len(roles) > 0 && roles[0] != ROLE_ALL
	agents := make([][]int, 0, 10)
	for key, _ := range producers {
		var gid, nid int
		fmt.Sscanf(key, "%d:%d", &gid, &nid)
		if onlyGid > 0 && onlyGid != gid {
			continue
		}

		if checkRole {
			agentRoles := producersRoles[key]
			match := true
			for _, r := range roles {
				if !In(agentRoles, r) {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}
		agents = append(agents, []int{gid, nid})
	}

	return agents
}

func sendResult(result *CommandResult) {
	db := pool.Get()
	defer db.Close()

	key := fmt.Sprintf("%d:%d", result.Gid, result.Nid)
	if data, err := json.Marshal(&result); err == nil {
		db.Do("HSET",
			fmt.Sprintf(JOBRESULT_HASH, result.Id),
			key,
			data)

		// push message to client result queue queue
		db.Do("RPUSH", getAgentResultQueue(result), data)
	}
}

func internal_list_agents(cmd *CommandMessage) (interface{}, error) {
	return producersRoles, nil
}

var internals = map[string]func(*CommandMessage) (interface{}, error){
	"list_agents": internal_list_agents,
}

func processInternalCommand(command CommandMessage) {
	result := &CommandResult{
		Id:        command.Id,
		Gid:       command.Gid,
		Nid:       command.Nid,
		State:     "ERROR",
		StartTime: int64(time.Duration(time.Now().UnixNano()) / time.Millisecond),
	}

	internal, ok := internals[command.Args.Name]
	if ok {
		data, err := internal(&command)
		if err != nil {
			result.Data = err.Error()
		} else {
			serialized, err := json.Marshal(data)
			if err != nil {
				result.Data = err.Error()
			}
			result.State = "SUCCESS"
			result.Data = string(serialized)
			result.Level = 20
		}
	} else {
		result.State = "UNKNOWN_CMD"
	}

	sendResult(result)
	signalQueues(command.Id)
}

func signalQueues(id string) {
	db := pool.Get()
	defer db.Close()
	db.Do("RPUSH", fmt.Sprintf(JOBS_QUEUED_QUEUE, id), "queued")
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

	if payload.Cmd == INTERNAL_COMMAND {
		go processInternalCommand(payload)
		return true
	}
	//sort command to the consumer queue.
	//either by role or by the gid/nid.
	ids := list.New()

	if len(payload.Roles) > 0 {
		//command has a given role
		active := getActiveAgents(payload.Gid, payload.Roles)
		if len(active) == 0 {
			//no active agents that saticifies this role.
			result := &CommandResult{
				Id:        payload.Id,
				Gid:       payload.Gid,
				Nid:       payload.Nid,
				State:     "ERROR",
				Data:      fmt.Sprintf("No agents with role '%v' alive!", payload.Roles),
				StartTime: int64(time.Duration(time.Now().UnixNano()) / time.Millisecond),
			}

			sendResult(result)
		} else {
			if payload.Fanout {
				//fanning out.
				for _, agent := range active {
					ids.PushBack(agent)
				}

			} else {
				agent := active[rand.Intn(len(active))]
				ids.PushBack(agent)
			}
		}
	} else {
		key := fmt.Sprintf("%d:%d", payload.Gid, payload.Nid)
		_, ok := producers[key]
		if !ok {
			//send error message to
			result := &CommandResult{
				Id:        payload.Id,
				Gid:       payload.Gid,
				Nid:       payload.Nid,
				State:     "ERROR",
				Data:      fmt.Sprintf("Agent is not alive!"),
				StartTime: int64(time.Duration(time.Now().UnixNano()) / time.Millisecond),
			}

			sendResult(result)
		} else {
			ids.PushBack([]int{payload.Gid, payload.Nid})
		}
	}

	// push logs
	if _, err := db.Do("LPUSH", LOG_QUEUE, command[1]); err != nil {
		log.Println("[-] log push error: ", err)
	}

	//distribution to agents.
	for e := ids.Front(); e != nil; e = e.Next() {
		// push message to client queue
		agent := e.Value.([]int)
		gid := agent[0]
		nid := agent[1]

		log.Println("Dispatching message to", agent)
		if _, err := db.Do("RPUSH", getAgentQueue(gid, nid), command[1]); err != nil {
			log.Println("[-] push error: ", err)
		}

		result_placehoder := CommandResult{
			Id:        payload.Id,
			Gid:       gid,
			Nid:       nid,
			State:     "QUEUED",
			StartTime: int64(time.Duration(time.Now().UnixNano()) / time.Millisecond),
		}

		if data, err := json.Marshal(&result_placehoder); err == nil {
			db.Do("HSET",
				fmt.Sprintf(JOBRESULT_HASH, payload.Id),
				fmt.Sprintf("%d:%d", gid, nid),
				data)
		}
	}

	signalQueues(payload.Id)
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
var producersRoles map[string][]string = make(map[string][]string)

// var activeRoles map[string]int = make(map[string]int)
// var activeGridRoles map[string]map[string]int = make(map[string]map[string]int)
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
				delete(producersRoles, key)
			}()

			for {
				if !func() bool {
					var data *PollData

					select {
					case data = <-producer:
					case <-time.After(AGENT_INACTIVER_AFTER_OVER):
						//no active agent for 10 min
						log.Println("Agent", key, "is inactive for over ", AGENT_INACTIVER_AFTER_OVER, ", cleaning up.")
						return false
					}

					msgChan := data.MsgChan
					defer close(msgChan)

					roles := data.Roles
					producersRoles[key] = roles

					db := pool.Get()
					defer db.Close()

					pending, err := redis.Strings(db.Do("BLPOP", getAgentQueue(igid, inid), "0"))
					if err != nil {
						if !isTimeout(err) {
							log.Println("Couldn't get new job for agent", key, err)
						}

						return true
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
	var payload CommandResult
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

	// push message to client main result queue
	db.Do("RPUSH", getAgentResultQueue(&payload), content)

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
			var value float64
			switch v := stats.Series[i][1].(type) {
			case int:
				value = float64(v)
			case float32:
				value = float64(v)
			case float64:
				value = v
			default:
				log.Println("Invalid influxdb value %v", v)
			}

			key := stats.Series[i][0].(string)
			//key is formated as gid.nid.cmd.domain.name.[measuerment] (6 parts)
			//so we can split it and then fill the gags.
			tags := make(map[string]string)
			tagsValues := strings.SplitN(key, ".", 6)
			for i, tagValue := range tagsValues {
				tags[TAGS[i]] = tagValue
			}

			point := influxdb.Point{
				Measurement: key,
				Time:        time.Unix(stats.Timestamp, 0),
				Tags:        tags,
				Fields: map[string]interface{}{
					"value": value,
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
		log.Println("INFLUXDB ERROR:", err)
		return
	}

	c.JSON(http.StatusOK, "ok")
}

func event(c *gin.Context) {
	gid := c.Param("gid")
	nid := c.Param("nid")

	log.Printf("[+] gin: event (gid: %s, nid: %s)\n", gid, nid)

	//force initializing of producer since the event is the first thing agent sends

	getProducerChan(gid, nid)

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

	stdout, err := cmd.StdoutPipe()
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
			if err != nil {
				log.Println(err)
			}

			cmdoutput, err := ioutil.ReadAll(stdout)
			if err != nil {
				log.Println(err)
			}

			err = cmd.Wait()
			if err != nil {
				log.Println("Failed to handle ", payload.Name, " event for agent: ", gid, nid, err)
				log.Println(string(cmdoutput))
				log.Println(string(cmderrors))
			}
		}()
	}

	c.JSON(http.StatusOK, "ok")
}

func handlHubbleProxy(context *gin.Context) {
	hublleProxy.ProxyHandler(context.Writer, context.Request)
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

	var err error
	settings, err = LoadSettingsFromTomlFile(cfg)
	if err != nil {
		log.Panicln("Error loading concfiguration file:", err)
	}

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
	router.GET("/:gid/:nid/hubble", handlHubbleProxy)
	// router.Static("/doc", "./doc")

	//start the builtin hubble agent so agents can open tunnels to master(controller) nodes.
	ipport := strings.Split(settings.Main.Listen, ":")
	if len(ipport) != 2 {
		log.Fatalf("Invalid listen address %s\n", settings.Main.Listen)
	}

	hubbleIp := "127.0.0.1"
	if ipport[0] != "" {
		hubbleIp = ipport[0]
	}

	wsURL := fmt.Sprintf("ws://%s:%s/0/0/hubble", hubbleIp, ipport[1])
	log.Println("Starting local hubble agent at", wsURL)
	agent := hublleAgent.NewAgent(wsURL, "controller", "", nil)
	var onExit func(agt hublleAgent.Agent, err error)

	onExit = func(agt hublleAgent.Agent, err error) {
		if err != nil {
			go func() {
				time.Sleep(3 * time.Second)
				agt.Start(onExit)
			}()
		}
	}

	agent.Start(onExit)

	server := &http.Server{Addr: settings.Main.Listen, Handler: router}
	if settings.TLSEnabled() {
		server.ListenAndServeTLS(settings.TLS.Cert, settings.TLS.Key)
	} else {
		log.Println("[WARNING] TLS not enabled, don't do this on production environments")
		server.ListenAndServe()
	}
}
