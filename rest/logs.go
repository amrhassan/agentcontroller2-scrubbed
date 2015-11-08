package rest
import (
	"github.com/gin-gonic/gin"
	"log"
	"io/ioutil"
	"net/http"
	"fmt"
)

func (rest *RestInterface) logs(c *gin.Context) {

	agentID := agentInformation(c)

	db := rest.pool.Get()
	defer db.Close()

	log.Printf("[+] gin: log (gid: %s, nid: %s)\n", agentID.GID, agentID.NID)

	// read body
	content, err := ioutil.ReadAll(c.Request.Body)

	if err != nil {
		log.Println("[-] cannot read body:", err)
		c.JSON(http.StatusInternalServerError, "error")
		return
	}

	// push body to redis
	id := fmt.Sprintf("%s:%s:log", agentID.GID, agentID.NID)
	log.Printf("[+] message destination [%s]\n", id)

	// push message to client queue
	_, err = db.Do("RPUSH", id, content)

	c.JSON(http.StatusOK, "ok")
}