package gonsqmgr

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
)

//NsqLookup struct
type NsqLookup struct {
	Nsqlookupdaddrs []string
	// Topics	[string]Topicslookup
}

// TopicLookup struct
type TopicLookup struct {
	Channels  []string    `json:"channels"`
	Producers []Producers `json:producers`
}
type NodesList struct {
	Producers []Producers `json:"producers"`
}

//Producers struct
type Producers struct {
	RemoteAddress    string   `json:"remote_address"`
	Hostname         string   `json:"hostname"`
	BroadcastAddress string   `json:"broadcast_address"`
	TCPPort          int      `json:"tcp_port"`
	HTTPPort         int      `json:"http_port"`
	Version          string   `json:"version"`
	Tombstones       []bool   `json:"tombstones"`
	Topics           []string `json:"topics"`
}

//NewNsqlookup new nsqlookup
func NewNsqlookup(addrs []string) (*NsqLookup, error) {
	if addrs != nil {
		nsqlookup := &NsqLookup{
			Nsqlookupdaddrs: addrs,
		}
		return nsqlookup, nil
	}

	return nil, errors.New("error nsq lookupd address")
}

func (nsqlookup *NsqLookup) getlookupaddr() string {
	return nsqlookup.Nsqlookupdaddrs[rand.Intn(len(nsqlookup.Nsqlookupdaddrs))]
}

//TopicList return topics list
func (nsqlookup *NsqLookup) TopicList() ([]string, error) {
	type topicmsg struct {
		Topics []string `json:"topics"`
	}
	urlstr := nsqlookup.getlookupaddr() + "/" + "topics"

	resp, err := http.Get(urlstr)
	if resp == nil {
		return nil, errors.New("Get topics error" + err.Error())
	}
	defer resp.Body.Close()
	s, err := ioutil.ReadAll(resp.Body)

	tmsg := &topicmsg{}
	if err := json.Unmarshal(s, tmsg); err != nil {
		return nil, err
	}
	return tmsg.Topics, nil
}

//LookupTopic return topiclookup
func (nsqlookup *NsqLookup) LookupTopic(topicname string) (*TopicLookup, error) {
	if !nsqlookup.istopicisexist(topicname) {
		return nil, errors.New("Topic is not exist yet ")
	}
	tplookup := &TopicLookup{}
	urlstr := nsqlookup.getlookupaddr() + "/" + "lookup?topic=" + topicname
	resp, err := http.Get(urlstr)
	if err != nil {
		return nil, errors.New("Get topics error" + err.Error())
	}
	defer resp.Body.Close()
	s, err := ioutil.ReadAll(resp.Body)
	if err := json.Unmarshal(s, tplookup); err != nil {
		return nil, err
	}
	return tplookup, nil
}

func (nsqlookup *NsqLookup) GetNodes() (*NodesList, error) {
	urlstr := nsqlookup.getlookupaddr() + "/" + "nodes"
	resp, err := http.Get(urlstr)
	if err != nil {
		return nil, errors.New("get node from nsqlookup error")
	}
	defer resp.Body.Close()
	nodes := &NodesList{}
	s, err := ioutil.ReadAll(resp.Body)
	if err := json.Unmarshal(s, nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

//Createtopic flags
const (
	EXCL   = int32(1)
	CREATE = int32(2)
)

// CreateTopic
// There has tow kind of flags EXCL and CREATE:
// EXCL: create a existed topic will return a error
// CREATE: create a topic,whatere if topic exist
// TODO;Tips:If applied to a distributed system,Need to introduce distributed locks
func (nsqlookup *NsqLookup) CreateTopic(topicname string, channels []string, flags int32 /*, nodenum int*/) (*NodesList, error) {
	if flags == EXCL {
		if nsqlookup.istopicisexist(topicname) {
			return nil, errors.New("Topic is exist")
		}
	} else if flags == CREATE {
		//TODO:
		//if topic is exist,retrun the exist nodes info
		if nsqlookup.istopicisexist(topicname) {
			lkuptopics, _ := nsqlookup.LookupTopic(topicname)
			if len(lkuptopics.Producers) == 0 {
				nlist, err := nsqlookup.createtopic(topicname, channels)
				if err != nil {
					return nil, err
				}
				return nlist, nil
			}
			return &NodesList{
				Producers: lkuptopics.Producers,
			}, nil
		}
		//if topic is not exist ,retrun nodes info by the func
		nlist, err := nsqlookup.createtopic(topicname, channels)
		if err != nil {
			return nil, err
		}
		return nlist, nil

	} else {
		return nil, errors.New("create flags is error")
	}
	return nil, nil
}

func (nsqlookup *NsqLookup) createtopic(topicname string, channels []string) (*NodesList, error) {
	urlstr := nsqlookup.getlookupaddr() + "/" + "/topic/create?topic=" + topicname
	_, err := http.Post(urlstr, "", nil)
	if err != nil {
		return nil, errors.New("Nsqlookup topic create API error")
	}
	//TODO:创建
	for _, ch := range channels {
		urlstrch := nsqlookup.getlookupaddr() + "/" + "/channel/create?topic=" + topicname + "&channel=" + ch
		_, err := http.Post(urlstrch, "", nil)
		if err != nil {
			return nil, errors.New("Nsqlookup channel carete API error")
		}
	}
	nodelist, err := nsqlookup.GetNodes()
	if err != nil {
		return nil, err
	}
	min := -1
	minidex := 0
	for index, val := range nodelist.Producers {
		if min == -1 || len(val.Topics) < min {
			min = len(val.Topics)
			minidex = index
		}
	}

	return &NodesList{
		[]Producers{
			nodelist.Producers[minidex],
		},
	}, nil
}

func (nsqlookup *NsqLookup) istopicisexist(topicname string) bool {
	topiclst, _ := nsqlookup.TopicList()
	for _, v := range topiclst {
		if topicname == v {
			return true
		}
	}
	return false
}

func isdumpsuffix(ch []string) bool {
	for _, vv := range ch {
		if strings.HasSuffix(vv, "_dump") {
			return true
		}
	}
	return false
}
