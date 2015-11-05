// AgentController's HTTP communication layer between Agent instances and itself
package rest
import (
	"github.com/gin-gonic/gin"
	"net/http"
	"github.com/garyburd/redigo/redis"
	"log"
	"github.com/amrhassan/agentcontroller2/core"
	"time"
	"github.com/amrhassan/agentcontroller2/agentpoll"
	"strconv"
	"encoding/json"
	"io/ioutil"
	"fmt"
	hublleProxy "github.com/Jumpscale/hubble/proxy"
	influxdb "github.com/influxdb/influxdb/client"
	"net/url"
	"strings"
	"os/exec"
	"github.com/amrhassan/agentcontroller2/settings"
)

const (
	hashCmdResults            = "jobresult:%s"
	cmdQueueAgentResponse     = "cmd.%s.%d.%d"
)

var influxDbTags = []string{"gid", "nid", "command", "domain", "name", "measurement"}

type RestInterface struct {
	pool *redis.Pool
	pollDataStreamManager *agentpoll.PollDataStreamManager
	router 		*gin.Engine
	settings 	*settings.Settings
}

func (rest *RestInterface) Router() *gin.Engine {
	return rest.router
}

func NewRestInterface(pool *redis.Pool, pollDataStreamManager *agentpoll.PollDataStreamManager,
	settings *settings.Settings) *RestInterface {

	rest := &RestInterface{
		pool: pool,
		pollDataStreamManager: pollDataStreamManager,
		router: gin.Default(),
		settings: settings,
	}

	rest.router.GET("/:gid/:nid/cmd", rest.cmd)
	rest.router.POST("/:gid/:nid/log", rest.logs)
	rest.router.POST("/:gid/:nid/result", rest.result)
	rest.router.POST("/:gid/:nid/stats", rest.stats)
	rest.router.POST("/:gid/:nid/event", rest.event)
	rest.router.GET("/:gid/:nid/hubble", rest.handlHubbleProxy)
	rest.router.GET("/:gid/:nid/script", rest.script)

	return rest
}

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

func (rest *RestInterface) getProducerChan(gid string, nid string) chan<- agentpoll.PollData {

	igid, err := strconv.Atoi(gid)
	if err != nil {
		panic(err)
	}
	inid, err := strconv.Atoi(nid)
	if err != nil {
		panic(err)
	}

	agentID := core.AgentID{GID: uint(igid), NID: uint(inid)}

	return rest.pollDataStreamManager.Get(agentID)
}

/*
Gets hashed scripts from redis.
*/
func (rest *RestInterface) script(c *gin.Context) {

	query := c.Request.URL.Query()
	hashes, ok := query["hash"]
	if !ok {
		// that's an error. Hash is required.
		c.String(http.StatusBadRequest, "Missing 'hash' param")
		return
	}

	hash := hashes[0]

	db := rest.pool.Get()
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

// REST stuff
func (rest *RestInterface) cmd(c *gin.Context) {
	gid := c.Param("gid")
	nid := c.Param("nid")

	query := c.Request.URL.Query()
	log.Printf("[+] gin: execute (gid: %s, nid: %s)\n", gid, nid)

	var roles []core.AgentRole
	for _, roleStr := range query["role"] {
		roles = append(roles, core.AgentRole(roleStr))
	}

	// listen for http closing
	notify := c.Writer.(http.CloseNotifier).CloseNotify()

	timeout := 60 * time.Second

	producer := rest.getProducerChan(gid, nid)

	data := agentpoll.PollData{
		Roles:   roles,
		CommandChannel: make(chan core.Command),
	}

	select {
	case producer <- data:
	case <-time.After(timeout):
		c.String(http.StatusOK, "")
		return
	}
	//at this point we are sure this is the ONLY agent polling on /gid/nid/cmd

	var command core.Command

	select {
	case command = <-data.CommandChannel:
	case <-notify:
	case <-time.After(timeout):
	}

	jsonCommand, err := json.Marshal(&command)
	if err != nil {
		panic(err)
	}

	c.String(http.StatusOK, string(jsonCommand))
}

func (rest *RestInterface) logs(c *gin.Context) {
	gid := c.Param("gid")
	nid := c.Param("nid")

	db := rest.pool.Get()
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

func getAgentResultQueue(result *core.CommandResult) string {
	return fmt.Sprintf(cmdQueueAgentResponse, result.ID, result.Gid, result.Nid)
}

func (rest *RestInterface) result(c *gin.Context) {
	gid := c.Param("gid")
	nid := c.Param("nid")

	key := fmt.Sprintf("%s:%s", gid, nid)
	db := rest.pool.Get()
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

func (rest *RestInterface) stats(c *gin.Context) {
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

	u, err := url.Parse(fmt.Sprintf("http://%s", rest.settings.Influxdb.Host))
	if err != nil {
		log.Println(err)
		return
	}

	// building Influxdb requests
	con, err := influxdb.NewClient(influxdb.Config{
		Username: rest.settings.Influxdb.User,
		Password: rest.settings.Influxdb.Password,
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
		Database:        rest.settings.Influxdb.Db,
		RetentionPolicy: "default",
	}

	if _, err := con.Write(batchPoints); err != nil {
		log.Println("INFLUXDB ERROR:", err)
		return
	}

	c.JSON(http.StatusOK, "ok")
}

func (rest *RestInterface) event(c *gin.Context) {
	gid := c.Param("gid")
	nid := c.Param("nid")

	log.Printf("[+] gin: event (gid: %s, nid: %s)\n", gid, nid)

	//force initializing of producer since the event is the first thing agent sends

	rest.getProducerChan(gid, nid)

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

	cmd := exec.Command(rest.settings.Handlers.Binary,
		fmt.Sprintf("%s.py", payload.Name), gid, nid)

	cmd.Dir = rest.settings.Handlers.Cwd
	//build env string
	var env []string
	if len(rest.settings.Handlers.Env) > 0 {
		env = make([]string, 0, len(rest.settings.Handlers.Env))
		for ek, ev := range rest.settings.Handlers.Env {
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



func (rest *RestInterface) handlHubbleProxy(context *gin.Context) {
	hublleProxy.ProxyHandler(context.Writer, context.Request)
}

