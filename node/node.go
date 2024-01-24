package node

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"

	"github.com/hikingpig/raft/consensus"
	raft_rpc "github.com/hikingpig/raft/rpc"
)

type Node struct {
	mu          sync.Mutex
	consen      raft_rpc.Consensus
	id          int
	peerIds     []int
	listener    net.Listener
	rpcServer   *rpc.Server
	peerClients map[int]*rpc.Client
	ready       <-chan struct{}
	quit        chan struct{}
	wg          sync.WaitGroup
}

func NewNode(id int, peerIds []int, ready <-chan struct{}) *Node {
	s := new(Node)
	s.id = id
	s.peerIds = peerIds
	s.peerClients = make(map[int]*rpc.Client)
	s.ready = ready
	s.quit = make(chan struct{})
	return s
}

func (n *Node) Start() {
	n.mu.Lock()
	n.consen = consensus.NewConsensus(n, n.peerIds, n.ready, n.quit)

	// Create a new RPC server and register a RPCProxy that forwards all methods
	// to n.cm
	n.rpcServer = rpc.NewServer()
	n.rpcServer.RegisterName("ConsensusModule", n.consen)

	var err error
	n.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	n.mu.Unlock()
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		for {
			conn, err := n.listener.Accept()
			if err != nil {
				select {
				case <-n.quit:
					return
				default:
					log.Fatal("accept error:", err)
				}
			}
			n.wg.Add(1)
			go func() {
				n.rpcServer.ServeConn(conn)
				n.wg.Done()
			}()
		}
	}()
}

// DisconnectAll closes all the client connections to peers for this node.
func (s *Node) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			s.peerClients[id].Close()
			s.peerClients[id] = nil
		}
	}
}

// Shutdown closes the server and waits for it to shut down properly.
func (s *Node) Shutdown() {
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

func (s *Node) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

func (s *Node) ConnectToPeer(peerId int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		s.peerClients[peerId] = client
	}
	return nil
}

// DisconnectPeer disconnects this server from the peer identified by peerId.
func (s *Node) DisconnectPeer(peerId int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] != nil {
		err := s.peerClients[peerId].Close()
		s.peerClients[peerId] = nil
		return err
	}
	return nil
}

func (s *Node) Call(id int, serviceMethod string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()

	// If this is called after shutdown (where client.Close is called), it will
	// return an error.
	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}
}

func (s *Node) ReportConsensus() (int, int, bool) {
	return s.consen.Report()
}
