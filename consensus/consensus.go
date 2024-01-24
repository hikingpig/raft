package consensus

import (
	"math/rand"
	"sync"
	"time"

	"github.com/hikingpig/raft/rpc"
	raft_rpc "github.com/hikingpig/raft/rpc"
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
}

func NewConsensus(node raft_rpc.Caller, peerIds []int, ready <-chan struct{}, quit <-chan struct{}) *consensus {
	c := &consensus{}
	c.node = node
	c.peerIds = peerIds
	c.quit = quit
	c.candidate = newCandidate(c)
	c.leader = newLeader(c)
	c.follower = newFollower(c)
	c.state = c.follower
	// start the control loop
	go func() {
		<-ready
		c.state.start()
	}()
	return c
}

// Report reports the state of this CM.
func (c *consensus) Report() (id int, term int, isLeader bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.id, c.term, c.state == c.leader
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
