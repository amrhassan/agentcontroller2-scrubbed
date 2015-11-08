package rest
import (
	"fmt"
	"github.com/gin-gonic/gin"
	"log"
	"io/ioutil"
"net/http"
"github.com/amrhassan/agentcontroller2/core"
"encoding/json"
)


func (rest *RestInterface) result(c *gin.Context) {

	id := agentInformation(c)

	key := fmt.Sprintf("%s:%s", id.GID, id.NID)
	db := rest.pool.Get()
	defer db.Close()

	log.Printf("[+] gin: result (gid: %s, nid: %s)\n", id.GID, id.NID)

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

