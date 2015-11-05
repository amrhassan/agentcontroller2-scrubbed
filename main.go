package main

import (
	"container/list"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
	hublleAgent "github.com/Jumpscale/hubble/agent"
	hubbleAuth "github.com/Jumpscale/hubble/auth"
	"github.com/garyburd/redigo/redis"
	"github.com/amrhassan/agentcontroller2/core"
	"github.com/amrhassan/agentcontroller2/redisdata"
	"github.com/amrhassan/agentcontroller2/agentdata"
	"github.com/amrhassan/agentcontroller2/agentpoll"
	"github.com/amrhassan/agentcontroller2/rest"
	"github.com/amrhassan/agentcontroller2/settings"
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

var commandStorage core.CommandStorage = redisdata.NewRedisCommandStorage(pool)
var incomingCommands core.Incoming = redisData
var commandLogger core.CommandLogger = redisData




// Returns the connected agents.
// If onlyGID is nonzero, returns only the agents with the specified onlyGID as their GID
// If roles is set and non-empty, returns only the agents with ALL of the specified roles
func getActiveAgents(onlyGid int, roles []string) []core.AgentID {
	var gidFilter *uint = nil
	var rolesFilter []core.AgentRole = nil

	if onlyGid > 0 {
		gid := uint(onlyGid)
		gidFilter = &gid
	}

	for _, role := range roles {
		rolesFilter = append(rolesFilter, core.AgentRole(role))
	}

	return agentData.FilteredConnectedAgents(gidFilter, rolesFilter)
}

func sendResult(result *core.CommandResult) {
	err := commandStorage.SetCommandResult(result)
	if err != nil {
		log.Println("[-] failed to publish command result: {}", err.Error())
	}
}

func internalListAgents(cmd *core.Command) (interface{}, error) {
//	return producersRoles, nil
	// What does this even do?
	// TODO
	panic("TODO")
}

var internals = map[string]func(*core.Command) (interface{}, error){
	"list_agents": internalListAgents,
}

func processInternalCommand(command *core.Command) {
	result := &core.CommandResult{
		ID:        command.ID,
		Gid:       command.Gid,
		Nid:       command.Nid,
		State:     core.COMMAND_STATE_ERROR,
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
				State:     core.COMMAND_STATE_ERROR,
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
//		key := fmt.Sprintf("%d:%d", command.Gid, command.Nid)
//		_, ok := producers[key]
		if ! agentData.IsConnected(core.AgentID{GID: uint(command.Gid), NID: uint(command.Nid)}) {
			//send error message to
			result := &core.CommandResult{
				ID:        command.ID,
				Gid:       command.Gid,
				Nid:       command.Nid,
				State:     core.COMMAND_STATE_ERROR,
				Data:      fmt.Sprintf("Agent is not alive!"),
				StartTime: int64(time.Duration(time.Now().UnixNano()) / time.Millisecond),
			}

			sendResult(result)
		} else {
			ids.PushBack([]int{command.Gid, command.Nid})
		}
	}

	// push logs
	if err := commandLogger.LogCommand(command); err != nil {
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


var agentData = agentdata.NewAgentData()
var pollDataStreamManager = agentpoll.NewManager(agentData, commandStorage)

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

func main() {
	var cfg string
	var globalSettings settings.Settings

	flag.StringVar(&cfg, "c", "", "Path to config file")
	flag.Parse()

	if cfg == "" {
		log.Println("Missing required option -c")
		flag.PrintDefaults()
		os.Exit(1)
	}

	var err error
	globalSettings, err = settings.LoadSettingsFromTomlFile(cfg)
	if err != nil {
		log.Panicln("Error loading configuration file:", err)
	}

	log.Printf("[+] redis server: <%s>\n", globalSettings.Main.RedisHost)

	pool = newPool(globalSettings.Main.RedisHost, globalSettings.Main.RedisPassword)

	db := pool.Get()
	if _, err := db.Do("PING"); err != nil {
		panic(fmt.Sprintf("Failed to connect to redis: %v", err))
	}

	db.Close()

	restInterface := rest.NewRestInterface(pool, pollDataStreamManager, &globalSettings)

	go cmdreader()

	//start schedular.
	scheduler := NewScheduler(pool)
	internals["scheduler_add"] = scheduler.Add
	internals["scheduler_list"] = scheduler.List
	internals["scheduler_remove"] = scheduler.Remove

	scheduler.Start()

	hubbleAuth.Install(hubbleAuth.NewAcceptAllModule())

	// router.Static("/doc", "./doc")

	var wg sync.WaitGroup
	wg.Add(len(globalSettings.Listen))
	for _, httpBinding := range globalSettings.Listen {
		go func(httpBinding settings.HTTPBinding) {
			server := &http.Server{Addr: httpBinding.Address, Handler: restInterface.Router()}
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

	StartSyncthingHubbleAgent(globalSettings.Syncthing.Port)

	wg.Wait()
}
