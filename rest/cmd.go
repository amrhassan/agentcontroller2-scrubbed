package rest
import (
	"github.com/amrhassan/agentcontroller2/core"
	"net/http"
	"log"
	"github.com/gin-gonic/gin"
	"time"
	"github.com/amrhassan/agentcontroller2/agentpoll"
	"encoding/json"
)

func (rest *RestInterface) cmd(c *gin.Context) {

	id := agentInformation(c)
	roles := agentRoles(c)

	log.Printf("[+] gin: execute (gid: %s, nid: %s)\n", id.GID, id.NID)

	// listen for http closing
	notify := c.Writer.(http.CloseNotifier).CloseNotify()

	timeout := 60 * time.Second

	producer := rest.getProducerChan(id)

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
