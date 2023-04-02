package main

import (
	"encoding/json"
	"log"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	SendType   = "send"
	SendOkType = "send_ok"

	PollType   = "poll"
	PollOkType = "poll_ok"

	CommitOffsetsType   = "commit_offsets"
	CommitOffsetsOkType = "commit_offsets_ok"

	ListCommittedOffsetsType   = "list_committed_offsets"
	ListCommittedOffsetsOkType = "list_committed_offsets_ok"
)

// RPC: `send`
// This message requests that a "msg" value be appended to a log identified by "key".
// Your node will receive a request message body that looks like this:
// {
//   "type": "send",
//   "key": "k1",
//   "msg": 123
// }
// In response, it should send an acknowledge with a `send_ok` message that contains the unique offset for the message in the log:
// {
//   "type": "send_ok",
//   "offset": 1000
// }

type SendBody struct {
	Type string `json:"type"`
	Key  string `json:"key"`
	Msg  int    `json:"msg"`
}

type SendOkBody struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
}

// RPC: `poll`
// This message requests that a node return messages from a set of logs starting from the given offset in each log.
// Your node will receive a request message body that looks like this:
// {
//   "type": "poll",
//   "offsets": {
//     "k1": 1000,
//     "k2": 2000
//   }
// }
// In response, it should return a `poll_ok` message with messages starting from the given offset for each log.
// Your server can choose to return as many messages for each log as it chooses:
// {
//   "type": "poll_ok",
//   "msgs": {
//     "k1": [[1000, 9], [1001, 5], [1002, 15]],
//     "k2": [[2000, 7], [2001, 2]]
//   }
// }

type PollBody struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type PollOkBody struct {
	Type string             `json:"type"`
	Msgs map[string][][]int `json:"msgs"`
}

// RPC: `commit_offsets`
// This message informs the node that messages have been successfully processed up to and including the given offset.
// Your node will receive a request message body that looks like this:
// {
//   "type": "commit_offsets",
//   "offsets": {
//     "k1": 1000,
//     "k2": 2000
//   }
// }
// In this example, the messages have been processed up to and including offset 1000 for log k1 and all messages up to and including offset 2000 for k2.
// In response, your node should return a `commit_offsets_ok` message body to acknowledge the request:
// {
//   "type": "commit_offsets_ok"
// }

type CommitOffsetsBody struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type CommitOffsetsOkBody struct {
	Type string `json:"type"`
}

// RPC: `list_committed_offsets`
// This message returns a map of committed offsets for a given set of logs.
// Clients use this to figure out where to start consuming from in a given log.
// Your node will receive a request message body that looks like this:
// {
//   "type": "list_committed_offsets",
//   "keys": ["k1", "k2"]
// }
// In response, your node should return a `list_committed_offsets_ok` message body containing a map of offsets for each requested key.
// Keys that do not exist on the node can be omitted.
// {
//   "type": "list_committed_offsets_ok",
//   "offsets": {
//     "k1": 1000,
//     "k2": 2000
//   }
// }

type ListCommittedOffsetsBody struct {
	Type string   `json:"type"`
	Keys []string `json:"keys"`
}

type ListCommittedOffsetsOkBody struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

func main() {
	type logs struct {
		mutex sync.RWMutex

		// {
		// 	"k1": 1000,
		// 	"k2": 2000
		// }
		offsets map[string]int

		// {
		// 	"k1": {
		// 		0: 1,
		// 		1: 123,
		// 		2: 12
		// 	},
		// 	"k2": {
		// 		0: 3,
		// 		1: 456,
		// 		2: 45
		// 	}
		// }
		msgs map[string]map[int]int
	}

	var committed logs
	var uncommitted logs

	uncommitted.offsets = make(map[string]int)
	uncommitted.msgs = make(map[string]map[int]int)

	committed.offsets = make(map[string]int)
	committed.msgs = make(map[string]map[int]int)

	node := maelstrom.NewNode()

	node.Handle(SendType, func(msg maelstrom.Message) error {
		var body SendBody

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		uncommitted.mutex.Lock()
		defer uncommitted.mutex.Unlock()

		if _, ok := uncommitted.msgs[body.Key]; !ok {
			uncommitted.msgs[body.Key] = make(map[int]int)
		}

		if _, ok := uncommitted.offsets[body.Key]; !ok {
			uncommitted.offsets[body.Key] = 0
		}

		offset := uncommitted.offsets[body.Key]

		uncommitted.msgs[body.Key][offset] = body.Msg

		err := node.Reply(msg, SendOkBody{
			Type:   SendOkType,
			Offset: offset,
		})

		if err != nil {
			return err
		}

		offset++

		uncommitted.offsets[body.Key] = offset

		return nil
	})

	node.Handle(PollType, func(msg maelstrom.Message) error {
		var body PollBody

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgs := make(map[string][][]int)

		uncommitted.mutex.RLock()
		defer uncommitted.mutex.RUnlock()

		for key, requestedOffset := range body.Offsets {
			keyMsgs, ok := uncommitted.msgs[key]

			if !ok {
				continue
			}

			offsets := []int{}

			for offset := range keyMsgs {
				if offset >= requestedOffset {
					offsets = append(offsets, offset)
				}
			}

			sort.Ints(offsets)

			for _, offset := range offsets {
				if msg, ok := keyMsgs[offset]; ok {
					msgs[key] = append(msgs[key], []int{offset, msg})
				}
			}
		}

		return node.Reply(msg, PollOkBody{
			Type: PollOkType,
			Msgs: msgs,
		})
	})

	node.Handle(CommitOffsetsType, func(msg maelstrom.Message) error {
		var body CommitOffsetsBody

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		uncommitted.mutex.RLock()
		defer uncommitted.mutex.RUnlock()

		committed.mutex.Lock()
		defer committed.mutex.Unlock()

		for key, requestedOffset := range body.Offsets {
			uncommittedKeyMsgs, ok := uncommitted.msgs[key]

			if !ok {
				continue
			}

			for offset, uncommittedMsg := range uncommittedKeyMsgs {
				// Commit only until requested offset
				if offset > requestedOffset {
					continue
				}

				if _, ok := committed.msgs[key]; !ok {
					committed.msgs[key] = make(map[int]int)
				}

				committed.msgs[key][offset] = uncommittedMsg
			}

			committed.offsets[key] = requestedOffset
		}

		return node.Reply(msg, CommitOffsetsOkBody{
			Type: CommitOffsetsOkType,
		})
	})

	node.Handle(ListCommittedOffsetsType, func(msg maelstrom.Message) error {
		var body ListCommittedOffsetsBody

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := make(map[string]int)

		committed.mutex.RLock()
		defer committed.mutex.RUnlock()

		for _, key := range body.Keys {
			if offset, ok := committed.offsets[key]; ok {
				offsets[key] = offset
			}
		}

		return node.Reply(msg, ListCommittedOffsetsOkBody{
			Type:    ListCommittedOffsetsOkType,
			Offsets: offsets,
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
