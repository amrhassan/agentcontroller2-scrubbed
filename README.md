# agentcontroller2
[![Build Status](https://travis-ci.org/amrhassan/agentcontroller2.svg?branch=master)](https://travis-ci.org/amrhassan/agentcontroller2)

JumpScale agentcontroller2 in Go

# Installation
```
go get github.com/amrhassan/agentcontroller2
```

# Running jsagencontroller
```
go run main.go -c agentcontroller2.toml
```

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
* Wait for commands from *cmds_queue* queue
* Decode JSON from this queue
* Push JSON on the right queue based on GID:NID
