The nsq manager sdk
====
The gonsqmanager package provides nsq managerment baseon nsqlookupd
The goal is to make nsq (a loose architecture) looks more like a distributed(system) queue.

Usage
=====
This is an example of using the gonsqmanager:

```go
package main

import (
	"fmt"
	"gonsqmgr/nsqmgr"
	"log"
)

func main() {
	nsqlk, _ := gonsqmgr.NewNsqlookup([]string{"http://127.0.0.1:4161"})
	
	topiclookup, err := nsqlk.LookupTopic("test")
	if err != nil {
		log.Fatal(err)
	}

	nodes, _ := nsqlk.GetNodes()
	
	createnodes, err := nsqlk.CreateTopic("test2", nil, gonsqmgr.CREATE)
	if err != nil {
		log.Fatal(err)
	}

}

```