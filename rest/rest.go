// AgentController's HTTP communication layer between Agent instances and itself
package rest
import (
	"github.com/gin-gonic/gin"
	"github.com/garyburd/redigo/redis"
	"github.com/amrhassan/agentcontroller2/core"
	"github.com/amrhassan/agentcontroller2/agentpoll"
	"fmt"
	hublleProxy "github.com/Jumpscale/hubble/proxy"
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

	agentGroup := rest.router.Group("/:gid/:nid")

	agentGroup.GET("/cmd", rest.cmd)
	agentGroup.POST("/log", rest.logs)
	agentGroup.POST("/result", rest.result)
	agentGroup.POST("/stats", rest.stats)
	agentGroup.POST("/event", rest.event)
	agentGroup.GET("/hubble", rest.handlHubbleProxy)
	agentGroup.GET("/script", rest.script)

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

// Extracts Agent information as provided in an HTTP request
func agentInformation(ctx *gin.Context) core.AgentID {

	var gid, nid uint

	fmt.Sscanf(ctx.Param("gid"), "%d", &gid)
	fmt.Sscanf(ctx.Param("nid"), "%d", &nid)

	return core.AgentID{GID: gid, NID: nid}
}

// Extracts Agent-provided roles fron an HTTP request as query parameters
func agentRoles(ctx *gin.Context) []core.AgentRole {

	query := ctx.Request.URL.Query()

	var roles []core.AgentRole
	for _, roleStr := range query["role"] {
		roles = append(roles, core.AgentRole(roleStr))
	}

	return roles
}

func (rest *RestInterface) getProducerChan(agentID core.AgentID) chan<- agentpoll.PollData {
	return rest.pollDataStreamManager.Get(agentID)
}


func getAgentResultQueue(result *core.CommandResult) string {
	return fmt.Sprintf(cmdQueueAgentResponse, result.ID, result.Gid, result.Nid)
}

func (rest *RestInterface) handlHubbleProxy(context *gin.Context) {
	hublleProxy.ProxyHandler(context.Writer, context.Request)
}

