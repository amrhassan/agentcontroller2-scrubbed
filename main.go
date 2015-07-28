package main

import (
    "os"
    "log"
    "fmt"
    "flag"
    "time"
    "net/http"
    "io/ioutil"
    "github.com/gin-gonic/gin"
    "github.com/garyburd/redigo/redis"
    "github.com/naoina/toml"
    "encoding/json"
    "os/exec"
    "github.com/Jumpscale/jsagentcontroller/influxdb-client-0.8.8"
)

type Settings struct {
    Main struct {
        Listen string
        RedisHost string
        RedisPassword string
    }

    Influxdb struct {
        Host string
        Db string
        User string
        Password string
    }

    Handlers struct {
        Binary string
        Cwd string
        Env map[string]string
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
    Id   string  `json:"id"`
    Gid  int     `json:"gid"`
    Nid  int     `json:"nid"`
}

type StatsRequest struct {
    Timestamp int64             `json:"timestamp"`
    Series    [][]interface{} `json:"series"`
}

type EvenRequest struct {
    Name string `json:"name"`
    Data string `json:"data"`
}

// redis stuff
func newPool(addr string, password string) *redis.Pool {
    return &redis.Pool {
        MaxIdle: 80,
        MaxActive: 12000,
        Dial: func() (redis.Conn, error) {
            c, err := redis.Dial("tcp", addr)

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

// Command Reader
func cmdreader() {
    db := pool.Get()
        defer db.Close()

    for {
        // waiting message from master queue
        command, err := redis.Strings(db.Do("BLPOP", "cmds_queue", "0"))

        log.Println("[+] message from master redis queue")

        if err != nil {
            log.Println("[-] pop error: ", err)
            continue
        }

        log.Println("[+] message payload: ", command[1])

        // parsing json data
        var payload CommandMessage
        err = json.Unmarshal([]byte(command[1]), &payload)

        if err != nil {
            log.Println("[-] message decoding: ", err)
            continue
        }

        id := fmt.Sprintf("%d:%d", payload.Gid, payload.Nid)
        log.Printf("[+] message destination [%s]\n", id)


        // push message to client queue
        _, err = db.Do("RPUSH", id, command[1])

        if err != nil {
            log.Println("[-] push error: ", err)
        }
    }
}

// REST stuff
func cmd(c *gin.Context) {
    ack := true

    gid := c.Param("gid")
    nid := c.Param("nid")

    log.Printf("[+] gin: execute (gid: %s, nid: %s)\n", gid, nid)

    // listen for http closing
    notify := c.Writer.(http.CloseNotifier).CloseNotify()

    go func() {
        <-notify
        ack = false
    }()

    id := fmt.Sprintf("%s:%s", gid, nid)
    log.Printf("[+] waiting data from [%s]\n", id)

    response := make(chan []string)

    db := pool.Get()
    defer db.Close()

    go func() {
        pending, err := redis.Strings(db.Do("BLPOP", id, "0"))
        if err != nil {
            return
        }
        response <- pending
        close(response)
    }()

    var payload string

    select {
        case pending := <- response:
            // extracting data from redis response
            payload = pending[1]
            log.Printf("[+] payload: %s\n", payload)
            // checking if connection alive
            if !ack {
                // push request back on queue
                fmt.Println("[-] connection is dead, replaying")
                db.Do("LPUSH", "cmds_queue", payload)
            }
        case <- time.After(120 * time.Second):
            payload = ""
    }

    // http reply
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

    log.Printf("[+] payload: jobid: %d\n", payload.Id)

    // push body to redis
    log.Printf("[+] message destination [%s]\n", payload.Id)


    // push message to client queue
    _, err = db.Do("RPUSH", fmt.Sprintf("cmds_queue_%s", payload.Id), content)

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
    var payload StatsRequest
    err = json.Unmarshal(content, &payload)

    if err != nil {
        log.Println("[-] cannot read json:", err)
        c.JSON(http.StatusInternalServerError, "json error")
        return
    }

    // building Influxdb requests
    con, err := client.NewClient(&client.ClientConfig{
        Username: settings.Influxdb.User,
        Password: settings.Influxdb.Password,
        Database: settings.Influxdb.Db,
        Host:     settings.Influxdb.Host,
    })

    if err != nil {
        log.Println(err)
    }

    var timestamp = payload.Timestamp
    seriesList := make([]*client.Series, len(payload.Series))

    for i := 0; i < len(payload.Series); i++ {
        series := &client.Series{
            Name: payload.Series[i][0].(string),
            Columns: []string{"time", "value"},
            // FIXME: add all points then write once
            Points: [][]interface{} {{
                timestamp * 1000, //influx expects time in ms
                payload.Series[i][1],
            },},
        }

        seriesList[i] =  series
    }

    if err := con.WriteSeries(seriesList); err != nil {
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
        go func(){
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
    router := gin.Default()

    go cmdreader()

    router.GET("/:gid/:nid/cmd", cmd)
    router.POST("/:gid/:nid/log", logs)
    router.POST("/:gid/:nid/result", result)
    router.POST("/:gid/:nid/stats", stats)
    router.POST("/:gid/:nid/event", event)
    // router.Static("/doc", "./doc")

    router.Run(settings.Main.Listen)
}
