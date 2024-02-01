package consensus

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/hikingpig/raft/rpc"
	raft_rpc "github.com/hikingpig/raft/rpc"
	"github.com/hikingpig/raft/storage"
)

// consensus is a state machine
// its state is switched between candidate, follower and leader
type consensus struct {
	candidate, follower, leader state
	state                       state
	mu                          sync.Mutex
	id                          int
	lastHeard                   time.Time
	term                        int
	peerIds                     []int
	node                        raft_rpc.Caller
	quit                        <-chan struct{}
	commitChan                  chan<- rpc.CommitEntry
	commitSignal                chan struct{}
	log                         []rpc.LogEntry
	commitIndex                 int
	lastApplied                 int
	nextIndex                   map[int]int
	matchIndex                  map[int]int
	storage                     storage.Storage
}

func NewConsensus(node raft_rpc.Caller, peerIds []int, ready <-chan struct{}, quit <-chan struct{}, commitChan chan<- rpc.CommitEntry, commitSignal chan struct{}, storage storage.Storage) *consensus {
	c := &consensus{}
	c.node = node
	c.peerIds = peerIds
	c.quit = quit
	c.candidate = newCandidate(c)
	c.leader = newLeader(c)
	c.follower = newFollower(c)
	c.state = c.follower
	c.commitChan = commitChan
	c.commitSignal = commitSignal
	c.storage = storage
	c.commitIndex = -1
	c.lastApplied = -1
	c.nextIndex = make(map[int]int)
	c.matchIndex = make(map[int]int)

	if c.storage.HasData() {
		fmt.Println("============= has data")
		c.restoreFromStorage()
	}
	// start the control loop
	go func() {
		<-ready
		c.lastHeard = time.Now() // needed ???
		c.state.start()
	}()
	go c.sendCommits()

	return c
}

// Report reports the state of this CM.
func (c *consensus) Report() (id int, term int, isLeader bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.id, c.term, c.state == c.leader
}

func (c *consensus) Submit(command interface{}) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == c.leader {
		c.log = append(c.log, rpc.LogEntry{Command: command, Term: c.term})
		c.persistToStorage()
		return true
	}
	return false
}

func (c *consensus) restoreFromStorage() {
	if termData, found := c.storage.Get("term"); found {
		d := gob.NewDecoder(bytes.NewBuffer(termData))
		if err := d.Decode(&c.term); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("term not found in storage")
	}
	if logData, found := c.storage.Get("log"); found {
		d := gob.NewDecoder(bytes.NewBuffer(logData))
		if err := d.Decode(&c.log); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("log not found in storage")
	}
	fmt.Printf("====== restore from storage, term: %d, log: %v\n", c.term, c.log)
}

func (c *consensus) persistToStorage() {
	var term bytes.Buffer
	if err := gob.NewEncoder(&term).Encode(c.term); err != nil {
		log.Fatal(err)
	}
	c.storage.Set("term", term.Bytes())

	var logData bytes.Buffer
	if err := gob.NewEncoder(&logData).Encode(c.log); err != nil {
		log.Fatal(err)
	}
	c.storage.Set("log", logData.Bytes())
}

func (c *consensus) RequestVote(args rpc.RequestVoteArgs, reply *rpc.RequestVoteReply) error {
	return c.state.requestVote(args, reply)
}

func (c *consensus) AppendEntries(args rpc.AppendEntriesArgs, reply *rpc.AppendEntriesReply) error {
	return c.state.appendEntries(args, reply)
}

func (c *consensus) timeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// updateState changes the state of consensus to a new state
// and starts the control loop of new state
// must acquire lock before calling
func (c *consensus) updateState(state state) {
	c.state = state
	c.state.start()
}

func (c *consensus) sendCommits() {
	for range c.commitSignal {
		// Find which entries we have to apply.
		c.mu.Lock()
		term := c.term
		lastApplied := c.lastApplied
		var entries []rpc.LogEntry
		if c.commitIndex > c.lastApplied {
			entries = c.log[c.lastApplied+1 : c.commitIndex+1]
			c.lastApplied = c.commitIndex
		}
		c.mu.Unlock()

		for i, entry := range entries {
			c.commitChan <- rpc.CommitEntry{
				Command: entry.Command,
				Index:   lastApplied + i + 1,
				Term:    term,
			}
		}
	}
}

// lastLogIndexAndTerm should only be used when a lock is already acquired!
func (c *consensus) lastLogIndexAndTerm() (int, int) {
	if len(c.log) > 0 {
		lastIndex := len(c.log) - 1
		return lastIndex, c.log[lastIndex].Term
	}
	return -1, -1
}

// isLogUpToDate should only be used when a lock is already acquired!
func (c *consensus) isLogUpToDate(args rpc.RequestVoteArgs) bool {
	lastLogIndex, lastLogTerm := c.lastLogIndexAndTerm()
	return args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
}

// isLogEntriesValid should only be used when a lock is already acquired!
func (c *consensus) isLogEntriesValid(args rpc.AppendEntriesArgs) bool {
	return args.PrevLogIndex == -1 ||
		(args.PrevLogIndex < len(c.log) && args.PrevLogTerm == c.log[args.PrevLogIndex].Term)
}

// sendAE is used when node is in "Leader" state
func (c *consensus) sendAE() {
	// sending AE requests to peers
	for _, peerId := range c.peerIds {
		// preparing args
		c.mu.Lock()
		ni := c.nextIndex[peerId]
		prevLogIndex := ni - 1
		prevLogTerm := -1
		if prevLogIndex >= 0 {
			prevLogTerm = c.log[prevLogIndex].Term
		}
		entries := c.log[ni:]
		args := rpc.AppendEntriesArgs{
			Term:         c.term,
			LeaderId:     c.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: c.commitIndex,
		}
		c.mu.Unlock()
		go func(peerId int) {
			var reply rpc.AppendEntriesReply
			if err := c.node.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				c.mu.Lock()
				defer c.mu.Unlock()
				// handle reply
				if reply.Term > c.term { // compare to current term, not only savedTerm
					c.term = reply.Term
					if c.state == c.leader { // only transitions to follower from leader
						// must update election event, else it will try to be candidate soon.
						c.lastHeard = time.Now()
						c.updateState(c.follower)
					}
					return
				}
				if c.state == c.leader && reply.Term == c.term {
					if reply.Success {
						// update nextIndex and matchIndex
						c.nextIndex[peerId] = ni + len(entries)
						c.matchIndex[peerId] = c.nextIndex[peerId] - 1
						commitIndex := c.commitIndex
						// find new commitIndex
						for i := c.commitIndex + 1; i < len(c.log); i++ {
							if c.log[i].Term == c.term {
								matchCount := 1
								// check if log[i] is replicated on majority of nodes
								for _, peerId := range c.peerIds {
									if c.matchIndex[peerId] >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(c.peerIds)+1 {
									c.commitIndex = i
								}
							}
						}
						if c.commitIndex != commitIndex {
							c.commitSignal <- struct{}{}
						}
					} else {
						c.nextIndex[peerId] = ni - 1
					}
				}
			}
		}(peerId)
	}
}
