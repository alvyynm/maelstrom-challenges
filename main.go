package main

import (
	"encoding/json"
	"log"
	"math/rand"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var neighbors []string
var seen = make(map[any]bool)

func main() {
	n := maelstrom.NewNode()

	n.Handle("echo", func(msg maelstrom.Message) error {
		// Unmarshal the message body as a loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "echo_ok"

		// echo the original message back with the updated message type
		return n.Reply(msg, body)
	})

	// CHALLENGE TWO: Unique IDs
	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// update the message type to return back
		body["type"] = "generate_ok"
		body["id"] = rand.Intn(1000)

		// send back a response
		return n.Reply(msg, body)
	})

	// Challenge for reading broadcast messages and returning them
	// Part 1: Read RPC broadcast message and acknowledge it
	var values []any
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		val := body["message"]

		if seen[val] {
			return nil
		}

		seen[val] = true

		values = append(values, body["message"])

		for _, neighbor := range neighbors {
			msg := map[string]any{
				"type":    "broadcast",
				"message": val,
			}
			n.Send(neighbor, msg)
		}

		reply := map[string]any{
			"type":        "broadcast_ok",
			"in_reply_to": body["msg_id"],
		}
		return n.Reply(msg, reply)
	})

	// Part two: Return all the values read from the RPC broadcast
	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = values
		return n.Reply(msg, body)
	})

	// Part three: Topology
	n.Handle("topology", func(msg maelstrom.Message) error {
		var body struct {
			Type     string              `json:"type"`
			Topology map[string][]string `json:"topology"`
			MsgID    int                 `json:"msg_id"`
		}
		// var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			log.Println("unmarshal error:", err)
			return err
		}

		neighbors = body.Topology[n.ID()]
		log.Printf("My neighbors are: %v", neighbors)

		// body["type"] = "topology_ok"
		reply := map[string]any{
			"type":        "topology_ok",
			"in_reply_to": body.MsgID,
		}

		return n.Reply(msg, reply)
	})

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error  {
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
