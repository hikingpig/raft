1. if a node is disconnected from the cluster, it keeps increasing its term. then it suddenly connects to a node in the cluster, a follower node. this follower receives RV request with higher term, it will update its term and vote for that node. the leader sends heartbeat to this follower, discovers that its term is outdated, it will become follower. there's no heartbeat until timeout and some nodes become candidate.
    - assume one node start an election early and win majority of votes, it becomes the leader and start sending heartbeat to the node
    
2. in candidate state, if the node started all goroutines to request vote then it converts to follower state in the middle... All remaining goroutine will still be processed, still assumming it is in candidate state. 2 corner cases can follows
    - another goroutine also change it to follower state. but with lower term!
        * we lock and access current term directly. this can not happen
    - it gets enough vote and transitions to leader state. but with lower term!
        * we lock and check reply's term with current term. this case is eliminated

3. can a node receive a rpc request in one state, then it transitions to another state and receive another rpc request. can these rpc requests interfere with one another? let's check some cases
    - a node is in follower state, it receives RV request from another node. it still in follower state but its term is updated. if it receives AE from leader, it will ask the leader to update term
    - a node is in follower state, it receives AE from leader, it processes the request successfully. it receivs RV request from another node ... similar to the above
    - a node is in candidate state. it receives AE from leader with higher term. it update its term and convert to follower state. it then receives RV request... similar to the above...   
        * the RV requests it send will also eventually return. 
            - If these happens after the state changes?
                - it will ignore them if the reply term is not larger (so it does not transition to follower state again)
                - 


4. can a node send RVs requests and later get enough votes to be leader. but another node already become a leader, with higher term?
