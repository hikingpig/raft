package consensus

import (
	"time"

	"github.com/hikingpig/raft/rpc"
)

// state is the consensus state: follower, candidate or leader
// rpc requests to consensus are delegated to its state
// whenever consensus changes to new state, a control loop of that state is started
// that monitors and controls `state transition` according to `state diagram` of raft
// when the consensus changes to new state, the control loop of old state will eventually exit
type state interface {
	start() // start the control loop of the state
	requestVote(args rpc.RequestVoteArgs, reply *rpc.RequestVoteReply) error
	appendEntries(args rpc.AppendEntriesArgs, reply *rpc.AppendEntriesReply) error
}

// follower is the state follower of a node in raft consensus
type follower struct {
	c *consensus
}

func newFollower(c *consensus) *follower {
	return &follower{
		c: c,
	}
}

// start starts the control loop for follower state
// if the control loop doesn't hear from leader long enough, it changes consensus state to candidate
// and start an election
func (f *follower) start() {
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		timeoutDuration := f.c.timeout()
		for {
			select {
			case <-f.c.quit: // exit control loop when node shutdown
				return
			case <-ticker.C:
				f.c.mu.Lock()
				elapsed := time.Since(f.c.lastHeard) // lastHeard is critical-section
				f.c.mu.Unlock()
				// the control loop exits when the node doesn't receive an election event for long enough
				if elapsed > timeoutDuration {
					f.c.mu.Lock() // eliminate locking? using channel?
					f.c.updateState(f.c.candidate)
					f.c.mu.Unlock()
					return
				}
			}
		}
	}()
}

// requestVote handles the RV rpc that is sent to consensus
func (f *follower) requestVote(args rpc.RequestVoteArgs, reply *rpc.RequestVoteReply) error {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	reply.VoteGranted = false
	reply.Term = f.c.term
	// only cast vote for the first time term is updated
	// for args.Term == f.c.Term, the vote is cast for previous RV requests
	if args.Term > f.c.term {
		f.c.term = args.Term
		reply.Term = f.c.term // update term even log is not up-to-date
		if f.c.isLogUpToDate(args) {
			reply.VoteGranted = true
			f.c.lastHeard = time.Now()
		}
	}
	return nil
}

// appendEntries handles AE rpc sent to node's consensus by leader node
func (f *follower) appendEntries(args rpc.AppendEntriesArgs, reply *rpc.AppendEntriesReply) error {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	reply.Success = false
	reply.Term = f.c.term
	if args.Term < f.c.term {
		return nil
	}
	// if a follower is disconnected and then reconnects. the cluster's leader and term may changed
	// it needs to update consensus's term
	if args.Term > f.c.term {
		f.c.term = args.Term
		reply.Term = f.c.term
	}
	if f.c.isLogEntriesValid(args) {
		f.c.lastHeard = time.Now()
		reply.Success = true
		f.c.log = append(f.c.log[:args.PrevLogIndex+1], args.Entries[:]...)
		// only update commitIndex if args'log is valid
		if args.LeaderCommit > f.c.commitIndex {
			f.c.commitIndex = min(args.LeaderCommit, len(f.c.log)-1)
			f.c.commitSignal <- struct{}{}
		}
	}
	return nil
}

func (f *follower) String() string {
	return "Follower"
}

type candidate struct {
	c *consensus
}

func newCandidate(c *consensus) *candidate {
	return &candidate{
		c: c,
	}
}

// start starts an election and the control loop that monitors the state of consensus
// it transitions to leader or follower if startElection is successful
// in case election takes longer than timeout duration, it starts another election and exits.
// the control loop can also terminate whenever it detects state change induced by rpc requests from other nodes
func (ca *candidate) start() {
	go func() {
		timeoutDuration := ca.c.timeout()
		ca.startElection()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ca.c.quit: // exit control loop when node shutdown
				return
			case <-ticker.C:
				ca.c.mu.Lock()
				state := ca.c.state
				elapsed := time.Since(ca.c.lastHeard)
				ca.c.mu.Unlock()
				// not in candidate state anymore, stops control loop
				if state != ca.c.candidate {
					return
				}
				// timeout, starts another election
				if elapsed > timeoutDuration {
					ca.start()
					return
				}
			}

		}
	}()
}

// startElection first increases the node's current term, and records election event
// it then starts several goroutines (in candidate state) then exits
// each goroutine tries to get vote from another peer by calling RequestVote RPC endpoint on that peer
// if the term of reply is higher than its current term, the node transitions to follower state
// if the node has enough votes, it transitions to leader state
func (ca *candidate) startElection() {
	ca.c.mu.Lock()
	ca.c.term++
	term := ca.c.term
	ca.c.lastHeard = time.Now()
	lastLogIndex, lastLogTerm := ca.c.lastLogIndexAndTerm()
	ca.c.mu.Unlock()
	votesReceived := 1
	// Send RequestVote RPCs to all other nodes concurrently.
	for _, peerId := range ca.c.peerIds {
		go func(peerId int) {
			args := rpc.RequestVoteArgs{
				Term:         term, // required lock!, accessing critical section
				CandidateId:  ca.c.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			var reply rpc.RequestVoteReply

			if err := ca.c.node.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				// lock after finishing call to allow rpc requests sent concurrently
				ca.c.mu.Lock()
				defer ca.c.mu.Unlock()
				// state changed, return immediately
				if ca.c.state != ca.c.candidate {
					return
				}
				if reply.Term > ca.c.term {
					ca.c.term = reply.Term
					ca.c.lastHeard = time.Now()
					// finds new term, transitions to follower state
					ca.c.updateState(ca.c.follower)
					return
				}
				// check reply term and current term. skip if the reply is outdated
				if reply.VoteGranted && reply.Term == ca.c.term {
					votesReceived++ // locking consensus also locks votesReceived!
					if votesReceived*2 > len(ca.c.peerIds)+1 {
						// won the election, transitions to leader state
						// no need check state again!
						ca.c.updateState(ca.c.leader)
					}
				}
			}
		}(peerId)
	}
}

// requestVote responds to RV rpc requests
// if the args.term > current term, it updates term, grants vote, records election event and transitions to follower state
// else, rejects vote and stays in candidate state
func (ca *candidate) requestVote(args rpc.RequestVoteArgs, reply *rpc.RequestVoteReply) error {
	ca.c.mu.Lock()
	defer ca.c.mu.Unlock() // locking applied, rpc requests are not handled concurrently
	reply.Term = ca.c.term
	reply.VoteGranted = false
	// only grants vote if args.term > current term
	// safely omits the field votedFor
	if args.Term > ca.c.term {
		ca.c.term = args.Term
		reply.Term = ca.c.term
		if ca.c.isLogUpToDate(args) {
			reply.VoteGranted = true
			ca.c.lastHeard = time.Now()
		}
		ca.c.updateState(ca.c.follower)
	}
	return nil
}

// appendEntries handles AppendEntries RPC request on behalf of the consensus module
// in candidate state, if the current term is lower than arg's term
// it updates term, records election event and convert to follower state
// else, it rejects the request
func (ca *candidate) appendEntries(args rpc.AppendEntriesArgs, reply *rpc.AppendEntriesReply) error {
	ca.c.mu.Lock()
	defer ca.c.mu.Unlock()
	reply.Success = false
	reply.Term = ca.c.term
	// args.Term == ca.c.term, another node won the election and become leader!
	// now it tells the candidate AE. the candidate should become follower
	if args.Term >= ca.c.term {
		ca.c.term = args.Term
		reply.Term = ca.c.term
		if ca.c.isLogEntriesValid(args) {
			reply.Success = true
			ca.c.log = append(ca.c.log[:args.PrevLogIndex+1], args.Entries[:]...)
			if args.LeaderCommit > ca.c.commitIndex {
				ca.c.commitIndex = min(args.LeaderCommit, len(ca.c.log)-1)
				ca.c.commitSignal <- struct{}{}
			}
		}
		ca.c.lastHeard = time.Now()
		ca.c.updateState(ca.c.follower)
	}
	return nil
}

func (ca *candidate) String() string {
	return "Candidate"
}

type leader struct {
	c *consensus
}

func newLeader(c *consensus) *leader {
	return &leader{
		c: c,
	}
}

// start sends heartbeats to peers every 50ms and monitors the state of consensus
// it the consensus's state is not leader, the control loop exits
func (l *leader) start() {
	go func() {
		l.c.mu.Lock()
		for _, peerId := range l.c.peerIds {
			l.c.nextIndex[peerId] = len(l.c.log)
			l.c.matchIndex[peerId] = -1
		}
		l.c.mu.Unlock()
		go func() {
			ticker := time.NewTicker(50 * time.Millisecond)
			defer ticker.Stop()
			for {
				l.c.mu.Lock()
				state := l.c.state
				l.c.mu.Unlock()
				// exit the loop immidiately when state changed
				if state != l.c.leader {
					return
				}
				// sending heartbeats to peers
				for _, peerId := range l.c.peerIds {
					// preparing args
					l.c.mu.Lock()
					ni := l.c.nextIndex[peerId]
					prevLogIndex := ni - 1
					prevLogTerm := -1
					if prevLogIndex >= 0 {
						prevLogTerm = l.c.log[prevLogIndex].Term
					}
					entries := l.c.log[ni:]
					args := rpc.AppendEntriesArgs{
						Term:         l.c.term,
						LeaderId:     l.c.id,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						Entries:      entries,
						LeaderCommit: l.c.commitIndex,
					}
					l.c.mu.Unlock()
					go func(peerId int) {
						var reply rpc.AppendEntriesReply
						if err := l.c.node.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
							l.c.mu.Lock()
							defer l.c.mu.Unlock()
							// handle reply
							if reply.Term > l.c.term { // compare to current term, not only savedTerm
								l.c.term = reply.Term
								if l.c.state == l.c.leader { // only transitions to follower from leader
									// must update election event, else it will try to be candidate soon.
									l.c.lastHeard = time.Now()
									l.c.updateState(l.c.follower)
								}
								return
							}
							if l.c.state == l.c.leader && reply.Term == l.c.term {
								if reply.Success {
									// update nextIndex and matchIndex
									l.c.nextIndex[peerId] = ni + len(entries)
									l.c.matchIndex[peerId] = l.c.nextIndex[peerId] - 1
									commitIndex := l.c.commitIndex
									// find new commitIndex
									for i := l.c.commitIndex + 1; i < len(l.c.log); i++ {
										if l.c.log[i].Term == l.c.term {
											matchCount := 1
											// check if log[i] is replicated on majority of nodes
											for _, peerId := range l.c.peerIds {
												if l.c.matchIndex[peerId] >= i {
													matchCount++
												}
											}
											if matchCount*2 > len(l.c.peerIds)+1 {
												l.c.commitIndex = i
											}
										}
									}
									if l.c.commitIndex != commitIndex {
										l.c.commitSignal <- struct{}{}
									}
								} else {
									l.c.nextIndex[peerId] = ni - 1
								}
							}
						}
					}(peerId)
				}
				select {
				case <-l.c.quit: // exit control loop when node shutdown
					return
				case <-ticker.C:
					continue
				}
			}
		}()
	}()
}

func (l *leader) requestVote(args rpc.RequestVoteArgs, reply *rpc.RequestVoteReply) error {
	l.c.mu.Lock()
	defer l.c.mu.Unlock() // locking applied, rpc requests are not handled concurrently
	reply.Term = l.c.term
	reply.VoteGranted = false
	if args.Term > l.c.term {
		l.c.term = args.Term
		reply.Term = l.c.term
		if l.c.isLogUpToDate(args) {
			reply.VoteGranted = true
			l.c.lastHeard = time.Now()
		}
		// step down from leader state
		l.c.updateState(l.c.follower)
	}
	return nil
}

// the leader node is disconnected from cluster and reconnects after a new leader has been elected
// the new leader will send AE rpc to the outdated leader node. it has to step down as follower
// similarly, the current leader can also receive AE request from disconnected leader, which has lower term
// it reply with higher term and reject the request. the control loop in the disconnected leader will make it step down
func (l *leader) appendEntries(args rpc.AppendEntriesArgs, reply *rpc.AppendEntriesReply) error {
	l.c.mu.Lock()
	defer l.c.mu.Unlock()
	reply.Success = false
	reply.Term = l.c.term
	if args.Term > l.c.term {
		l.c.term = args.Term
		reply.Term = l.c.term
		if l.c.isLogEntriesValid(args) {
			reply.Success = true
			l.c.log = append(l.c.log[:args.PrevLogIndex+1], args.Entries[:]...)
			if args.LeaderCommit > l.c.commitIndex {
				l.c.commitIndex = min(args.LeaderCommit, len(l.c.log)-1)
				l.c.commitSignal <- struct{}{}
			}
		}
		l.c.lastHeard = time.Now()
		l.c.updateState(l.c.follower)
	}

	return nil
}

func (l *leader) String() string {
	return "Leader"
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
