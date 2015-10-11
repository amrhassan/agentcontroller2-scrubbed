package main

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"log"
)

const (
	ScriptHashTimeout = 86400 // seconds
)

type Interceptor func(map[string]interface{}) (map[string]interface{}, error)

var interceptors = map[string]Interceptor{
	"jumpscript_content": JumpscriptHasherInterceptor,
}

/**
Hashes jumpscripts executed by the jumpscript_content and store it in memory.
**/
func JumpscriptHasherInterceptor(cmd map[string]interface{}) (map[string]interface{}, error) {
	datastr, ok := cmd["data"].(string)
	if !ok {
		return nil, errors.New("Expecting command 'data' to be string")
	}

	data := make(map[string]interface{})
	err := json.Unmarshal([]byte(datastr), &data)
	if err != nil {
		return nil, err
	}

	content, ok := data["content"]
	if !ok {
		return nil, errors.New("jumpscript_content doesn't have content payload")
	}

	contentstr, ok := content.(string)
	if !ok {
		return nil, errors.New("Expected 'content' to be string")
	}

	hash := fmt.Sprintf("%x", md5.Sum([]byte(contentstr)))

	db := pool.Get()
	defer db.Close()

	if _, err := db.Do("SET", hash, contentstr, "EX", ScriptHashTimeout); err != nil {
		return nil, err
	}

	//hash is stored. Now modify the command and forward it.
	delete(data, "content")
	data["hash"] = hash

	if datastr, err := json.Marshal(data); err != nil {
		return nil, err
	} else {
		cmd["data"] = string(datastr)
	}

	return cmd, nil
}

/*
Intercepts raw command data and manipulate it if needed.
*/
func InterceptCommand(command string) string {
	cmd := make(map[string]interface{})

	err := json.Unmarshal([]byte(command), &cmd)
	if err != nil {
		log.Println("Error: failed to intercept command", command, err)
	}

	cmdName, ok := cmd["cmd"].(string)
	if !ok {
		log.Println("Expected 'cmd' to be string")
		return command
	}

	interceptor, ok := interceptors[cmdName]
	if ok {
		cmd, err = interceptor(cmd)
		if err == nil {
			if data, err := json.Marshal(cmd); err == nil {
				command = string(data)
			} else {
				log.Println("Failed to serialize intercepted command", err)
			}
		} else {
			log.Println("Failed to intercept command", err)
		}
	}
	log.Println("Command:", command)
	return command
}
