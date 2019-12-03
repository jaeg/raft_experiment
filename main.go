package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/go-redis/redis/v7"
)

const namespace = "Raft"

var client *redis.Client

var isLeader = false
var isCandidate = false
var id = ""

func main() {
	client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pong, err := client.Ping().Result()
	fmt.Println(pong, err)

	startup()

	for {
		if isLeader {
			followers, err := client.HKeys(namespace + ":Nodes").Result()
			if err != nil {
				panic(err)
			}
			for f := range followers {
				fmt.Println(followers[f])
				if id != followers[f] {
					client.LPush(namespace+":"+followers[f], "Ping")
					_, err := client.BLPop(1*time.Second, namespace+":"+followers[f]+":Pong").Result()
					if err != nil {
						if err == redis.Nil {
							fmt.Println("No pong from " + followers[f])
						} else {
							panic(err)
						}
					}
				}

			}
			time.Sleep(1 * time.Second)
		} else if isCandidate {

		} else {
			hb, err := client.BLPop(10*time.Second, namespace+":"+id).Result()
			if err != nil {
				if err == redis.Nil {
					//Become leader
					fmt.Println("Timeout become leader")
					isLeader = true
				} else {
					panic(err)
				}
			}

			fmt.Println(hb)
		}

	}
}

func startup() {
	if client.HExists(namespace, "term").Val() == false {
		client.HSet(namespace, "term", 0)
	}
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)

	id = strconv.Itoa(r.Intn(100))
	client.HSet(namespace+":Nodes", id, "Follower")
}
