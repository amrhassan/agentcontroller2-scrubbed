# jsagentcontroller
JumpScale Agentcontroller in Go

# REST Service
Note: GID, NID and JID is extracted from URL or from JSON body

## GET /[gid]/[nid]/cmd
* If some commands are in redis queue (*$GID:$NID*), it's directly pushed
* If nothing is pending, waits (long poll) for a command from redis

## POST /[gid]/[nid]/log
* Push logs to redis queue (*$GID:$NID:LOG*)

## POST /[gid]/[nid]/result
* Push job result to redis queue (*$JID*)

## GET /[gid]/[nid]/stats
* Save logs in influxdb database
* Format: {timestamp: xxx, series: [[key, value], [key, value], ...]}

# Commands Reader
* Wait for commands from *\_\_master\_\_* queue
* Decode JSON from this queue
* Push JSON on the right queue based on GID:NID
