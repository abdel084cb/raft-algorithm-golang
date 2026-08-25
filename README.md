# Raft Consensus Algorithm in Go

Implementation of the **Raft distributed consensus algorithm** written in Go.

This project was developed as part of Computer Engineering studies to explore distributed systems and leader election.

## Features

- Leader election between Raft nodes
- Follower, Candidate and Leader states
- Randomized election timeouts
- Heartbeats between leader and followers
- Vote requests and term management
- Log replication using AppendEntries
- Log consistency checks and conflict resolution
- Majority-based commit mechanism
- Leader re-election after failure
- Basic replicated key-value operations
- Concurrent operation handling
- Integration tests with node failures and loss of quorum

## Technologies

* Go
* RPC
* Docker
* SSH-based deployment/testing (also Kubernetes)
