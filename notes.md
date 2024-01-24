- critical section data:
    - lastHeard
    - state

- in candidate state, when the node transitions to follower or leader state in startElection, the control loop in candidate state keeps running for a while after the control loop in follower or leader state started. after the control loop in candidate state detects the state change, it will stop.
    * if the control loop of candidate state gets timeout before it detects state change, it can start new election. but that would be rare, because to read or change state, the goroutine must acquire lock. if state changes, the lock released, then the control loop should be able to detect it. else, state is not changed and control loop of follower or candidate state is not started yet.

- because we use the lock to handle rpc requests, the more nodes we have, the more rpc requests need to be handle, the more the performance of the cluster will degrade.

- problems with the Node design
    - must provide peerIds when create node
    - node doesn't have option to join cluster
    - doesn't have a cluster object!
    - harness in testharness is actually a cluster!
        - peerIds must be supplied when create harness
        - the number of nodes is fixed by peerIds, we can not add or removed
        - election hold by candidate must be approved by majority of peerIds! even they are disconnected!