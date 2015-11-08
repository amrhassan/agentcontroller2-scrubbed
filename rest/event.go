package rest
import (
	"net/http"
	"github.com/gin-gonic/gin"
	"log"
	"io/ioutil"
"encoding/json"
	"os/exec"
	"fmt"
)

func (rest *RestInterface) event(c *gin.Context) {

	id := agentInformation(c)

	log.Printf("[+] gin: event (gid: %s, nid: %s)\n", id.GID, id.NID)

	//force initializing of producer since the event is the first thing agent sends

	rest.getProducerChan(id)

	content, err := ioutil.ReadAll(c.Request.Body)

	if err != nil {
		log.Println("[-] cannot read body:", err)
		c.JSON(http.StatusInternalServerError, "body error")
		return
	}

	var payload EvenRequest
	err = json.Unmarshal(content, &payload)
	if err != nil {
		log.Println(err)
		c.JSON(http.StatusInternalServerError, "Error")
	}

	cmd := exec.Command(rest.settings.Handlers.Binary,
		fmt.Sprintf("%s.py", payload.Name), fmt.Sprintf("%d", id.GID), fmt.Sprintf("%d", id.NID))

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

	log.Println("Starting handler for", payload.Name, "event, for agent", id.GID, id.NID)
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
				log.Println("Failed to handle ", payload.Name, " event for agent: ", id.GID, id.NID, err)
				log.Println(string(cmdoutput))
				log.Println(string(cmderrors))
			}
		}()
	}

	c.JSON(http.StatusOK, "ok")
}
