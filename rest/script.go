package rest
import (
	"github.com/gin-gonic/gin"
	"net/http"
	"github.com/garyburd/redigo/redis"
	"log"
)

// Gets hashed scripts from redis.
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
