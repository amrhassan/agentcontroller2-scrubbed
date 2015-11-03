package main

import (
	"container/list"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
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

	"github.com/amrhassan/agentcontroller2/core"
	"github.com/amrhassan/agentcontroller2/redisdata"
)

const (
	agentInteractiveAfterOver = 30 * time.Second
	roleAll                   = "*"
	cmdQueueMain              = "cmds.queue"
	cmdQueueCmdQueued         = "cmd.%s.queued"
	cmdQueueAgentResponse     = "cmd.%s.%d.%d"
	logQueue                  = "joblog"
	hashCmdResults            = "jobresult:%s"
	cmdInternal               = "controller"
)

var influxDbTags = []string{"gid", "nid", "command", "domain", "name", "measurement"}


//StatsRequest stats request
type StatsRequest struct {
	Timestamp int64           `json:"timestamp"`
	Series    [][]interface{} `json:"series"`
}

//EvenRequest event request
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
			c, err := redis.DialTimeout("tcp", addr, 0, agentInteractiveAfterOver/2, 0)

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
var redisData = redisdata.NewRedisData(pool)

var incomingCommands core.Incoming = redisData
var commandStorage core.CommandStorage = redisData
var logger core.CommandLogger = redisData


func isTimeout(err error) bool {
	return strings.Contains(err.Error(), "timeout")
}

func getAgentQueue(gid int, nid int) string {
	return fmt.Sprintf("cmds:%d:%d", gid, nid)
}

func getAgentResultQueue(result *core.CommandResult) string {
	return fmt.Sprintf(cmdQueueAgentResponse, result.ID, result.Gid, result.Nid)
}

func getActiveAgents(onlyGid int, roles []string) [][]int {
	producersLock.Lock()
	defer producersLock.Unlock()

	checkRole := len(roles) > 0 && roles[0] != roleAll
	agents := make([][]int, 0, 10)
	for key := range producers {
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

func sendResult(result *core.CommandResult) {
	err := redisData.SetCommandResult(result)
	if err != nil {
		log.Println("[-] failed to publish command result: {}", err.Error())
	}
}

func internalListAgents(cmd *core.Command) (interface{}, error) {
	return producersRoles, nil
}

var internals = map[string]func(*core.Command) (interface{}, error){
	"list_agents": internalListAgents,
}

func processInternalCommand(command *core.Command) {
	result := &core.CommandResult{
		ID:        command.ID,
		Gid:       command.Gid,
		Nid:       command.Nid,
		State:     "ERROR",
		StartTime: int64(time.Duration(time.Now().UnixNano()) / time.Millisecond),
	}

	internal, ok := internals[command.Args.Name]
	if ok {
		data, err := internal(command)
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
	signalQueues(command.ID)
}

func signalQueues(id string) {
	err := redisData.SignalCommandAsQueued(id)
	if err != nil {
		log.Printf("[-] failed to signal command as queued %s", err.Error())
	}
}

func readSingleCmd() bool {

	command, err := incomingCommands.ReceiveCommand()
	if err != nil {
		switch {
		case incomingCommands.IsCommandFormatError(err):
			log.Fatal("Incoming command id malformed: %v", err)
		default:
			log.Fatal("Data store error: %v", err)
		}
	}

	if command.Cmd == cmdInternal {
		go processInternalCommand(command)
		return true
	}

	//sort command to the consumer queue.
	//either by role or by the gid/nid.
	ids := list.New()

	if len(command.Roles) > 0 {
		//command has a given role
		active := getActiveAgents(command.Gid, command.Roles)
		if len(active) == 0 {
			//no active agents that saticifies this role.
			result := &core.CommandResult{
				ID:        command.ID,
				Gid:       command.Gid,
				Nid:       command.Nid,
				State:     "ERROR",
				Data:      fmt.Sprintf("No agents with role '%v' alive!", command.Roles),
				StartTime: int64(time.Duration(time.Now().UnixNano()) / time.Millisecond),
			}

			sendResult(result)
		} else {
			if command.Fanout {
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
		key := fmt.Sprintf("%d:%d", command.Gid, command.Nid)
		_, ok := producers[key]
		if !ok {
			//send error message to
			result := &core.CommandResult{
				ID:        command.ID,
				Gid:       command.Gid,
				Nid:       command.Nid,
				State:     "ERROR",
				Data:      fmt.Sprintf("Agent is not alive!"),
				StartTime: int64(time.Duration(time.Now().UnixNano()) / time.Millisecond),
			}

			sendResult(result)
		} else {
			ids.PushBack([]int{command.Gid, command.Nid})
		}
	}

	// push logs
	if err := logger.LogCommand(command); err != nil {
		log.Println("[-] log push error: ", err)
	}

	//distribution to agents.
	for e := ids.Front(); e != nil; e = e.Next() {
		// push message to client queue
		agent := e.Value.([]int)
		gid := agent[0]
		nid := agent[1]

		log.Println("Dispatching message to", agent)
		agentID := core.AgentID{GID: uint(gid), NID: uint(nid)}

		err := commandStorage.QueueReceivedCommand(agentID, command)
		if err != nil {
			log.Println("[-] push error: ", err)
		}

		err = redisData.RespondToCommandAsJustQueued(agentID, command)
		if err != nil {
			log.Println("[-] command response error: ", err)
		}

	}

	signalQueues(command.ID)
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

//In checks if x is in l
func In(l []string, x string) bool {
	for i := 0; i < len(l); i++ {
		if l[i] == x {
			return true
		}
	}

	return false
}

var producers = make(map[string]chan *PollData)
var producersRoles = make(map[string][]string)

// var activeRoles map[string]int = make(map[string]int)
// var activeGridRoles map[string]map[string]int = make(map[string]map[string]int)
var producersLock sync.Mutex

/*
PollData Gets a chain for the caller to wait on, we return a chan chan string instead
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
					case <-time.After(agentInteractiveAfterOver):
						//no active agent for 10 min
						log.Println("Agent", key, "is inactive for over ", agentInteractiveAfterOver, ", cleaning up.")
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
						var payload core.Command
						if err := json.Unmarshal([]byte(pending[1]), &payload); err != nil {
							break
						}

						resultPlacehoder := core.CommandResult{
							ID:        payload.ID,
							Gid:       igid,
							Nid:       inid,
							State:     "RUNNING",
							StartTime: int64(time.Duration(time.Now().UnixNano()) / time.Millisecond),
						}

						if data, err := json.Marshal(&resultPlacehoder); err == nil {
							db.Do("HSET",
								fmt.Sprintf(hashCmdResults, payload.ID),
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
	var payload core.CommandResult
	err = json.Unmarshal(content, &payload)

	if err != nil {
		log.Println("[-] cannot read json:", err)
		c.JSON(http.StatusInternalServerError, "json error")
		return
	}

	log.Println("Jobresult:", payload.ID)

	// update jobresult
	db.Do("HSET",
		fmt.Sprintf(hashCmdResults, payload.ID),
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
				log.Println("Invalid influxdb value:", v)
			}

			key := stats.Series[i][0].(string)
			//key is formated as gid.nid.cmd.domain.name.[measuerment] (6 parts)
			//so we can split it and then fill the gags.
			tags := make(map[string]string)
			tagsValues := strings.SplitN(key, ".", 6)
			for i, tagValue := range tagsValues {
				tags[influxDbTags[i]] = tagValue
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

/*
Gets hashed scripts from redis.
*/
func script(c *gin.Context) {

	query := c.Request.URL.Query()
	hashes, ok := query["hash"]
	if !ok {
		// that's an error. Hash is required.
		c.String(http.StatusBadRequest, "Missing 'hash' param")
		return
	}

	hash := hashes[0]

	db := pool.Get()
	defer db.Close()

	payload, err := redis.String(db.Do("GET", hash))
	if err != nil {
		log.Println("Script get error:", err)
		c.String(http.StatusNotFound, "Script with hash '%s' not found", hash)
		return
	}

	log.Println(payload)

	c.String(http.StatusOK, payload)
}

func handlHubbleProxy(context *gin.Context) {
	hublleProxy.ProxyHandler(context.Writer, context.Request)
}

//StartSyncthingHubbleAgent start the builtin hubble agent required for Syncthing
func StartSyncthingHubbleAgent(hubblePort int) {

	wsURL := fmt.Sprintf("ws://127.0.0.1:%d/0/0/hubble", hubblePort)
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
}

var settings Settings

func main() {
	var cfg string

	flag.StringVar(&cfg, "c", "", "Path to config file")
	flag.Parse()

	if cfg == "" {
		log.Println("Missing required option -c")
		flag.PrintDefaults()
		os.Exit(1)
	}

	var err error
	settings, err = LoadSettingsFromTomlFile(cfg)
	if err != nil {
		log.Panicln("Error loading configuration file:", err)
	}

	log.Printf("[+] redis server: <%s>\n", settings.Main.RedisHost)

	pool = newPool(settings.Main.RedisHost, settings.Main.RedisPassword)

	db := pool.Get()
	if _, err := db.Do("PING"); err != nil {
		panic(fmt.Sprintf("Failed to connect to redis: %v", err))
	}

	db.Close()

	router := gin.Default()

	go cmdreader()

	//start schedular.
	scheduler := NewScheduler(pool)
	internals["scheduler_add"] = scheduler.Add
	internals["scheduler_list"] = scheduler.List
	internals["scheduler_remove"] = scheduler.Remove

	scheduler.Start()

	hubbleAuth.Install(hubbleAuth.NewAcceptAllModule())
	router.GET("/:gid/:nid/cmd", cmd)
	router.POST("/:gid/:nid/log", logs)
	router.POST("/:gid/:nid/result", result)
	router.POST("/:gid/:nid/stats", stats)
	router.POST("/:gid/:nid/event", event)
	router.GET("/:gid/:nid/hubble", handlHubbleProxy)
	router.GET("/:gid/:nid/script", script)
	// router.Static("/doc", "./doc")

	var wg sync.WaitGroup
	wg.Add(len(settings.Listen))
	for _, httpBinding := range settings.Listen {
		go func(httpBinding HTTPBinding) {
			server := &http.Server{Addr: httpBinding.Address, Handler: router}
			if httpBinding.TLSEnabled() {
				server.TLSConfig = &tls.Config{}

				if err := configureServerCertificates(httpBinding, server); err != nil {
					log.Panicln("Unable to load the server certificates", err)
				}

				if err := configureClientCertificates(httpBinding, server); err != nil {
					log.Panicln("Unable to load the clientCA's", err)
				}

				ln, err := net.Listen("tcp", server.Addr)
				if err != nil {
					log.Panicln(err)
				}

				tlsListener := tls.NewListener(ln, server.TLSConfig)
				log.Println("Listening on", httpBinding.Address, "with TLS")
				if err := server.Serve(tlsListener); err != nil {
					log.Panicln(err)
				}
				wg.Done()
			} else {
				log.Println("Listening on", httpBinding.Address)
				if err := server.ListenAndServe(); err != nil {
					log.Panicln(err)
				}
				wg.Done()
			}
		}(httpBinding)
	}

	StartSyncthingHubbleAgent(settings.Syncthing.Port)

	wg.Wait()
}
