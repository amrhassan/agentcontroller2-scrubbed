package main

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"log"
)

const (
	scriptHashTimeout = 86400 // seconds
)

//Interceptor is a callback type
type Interceptor func(map[string]interface{}) (map[string]interface{}, error)

var interceptors = map[string]Interceptor{
	"jumpscript_content": JumpscriptHasherInterceptor,
}

/*
JumpscriptHasherInterceptor hashes jumpscripts executed by the jumpscript_content and store it in redis. Alters the passed command as needed
*/
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

	if _, err := db.Do("SET", hash, contentstr, "EX", scriptHashTimeout); err != nil {
		return nil, err
	}

	//hash is stored. Now modify the command and forward it.
	delete(data, "content")
	data["hash"] = hash

	updatedDatastr, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	cmd["data"] = string(updatedDatastr)

	return cmd, nil
}

/*
InterceptCommand intercepts raw command data and manipulate it if needed.
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
