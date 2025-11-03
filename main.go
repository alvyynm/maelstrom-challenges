package main

import (
	"encoding/json"
	"log"
	"math/rand"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

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

		values = append(values, body["message"])
		reply := map[string]any {
			"type": "broadcast_ok",
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
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			log.Println("unmarshal error:", err)
			return err
		}

		body["type"] = "topology_ok"
		reply := map[string]any {
			"type": "topology_ok",
			"in_reply_to": body["msg_id"],
		}

		return n.Reply(msg, reply)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
