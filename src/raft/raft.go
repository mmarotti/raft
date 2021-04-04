package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Persistent state
	state       string
	currentTerm int
	votedFor    int
	log         []int

	// Volatile state
	commitIndex                           int
	lastApplied                           int
	receivedAppendEntriesInCurrentTimeout bool
	currentVotes                          int

	// Volatile state (leader)
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type RespondVoteReply struct{}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// fmt.Println("Received vote request", rf.me, args.Term, args.CandidateId)

	reply.Term = args.Term
	reply.VoteGranted = rf.votedFor == -1 && args.Term >= rf.currentTerm // Haven't voted yet on this term

	responseSuccess := rf.sendRespondVote(args.CandidateId, reply, &RespondVoteReply{})

	if reply.VoteGranted && responseSuccess {
		rf.votedFor = args.CandidateId
	}
}

func (rf *Raft) RespondVote(args *RequestVoteReply, reply *RespondVoteReply) {
	// fmt.Println("Received vote response", rf.me, args.VoteGranted)

	if args.VoteGranted {
		rf.currentVotes++
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendRespondVote(server int, args *RequestVoteReply, reply *RespondVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RespondVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	fmt.Println("Creating new Raft server", peers, me)

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = "follower"
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	// There's no null value for int, as peers starts at 0 we are using -1 as null
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionTimeout()

	return rf
}

func (rf *Raft) electionTimeout() {
	// Store if server received AppendEntries from leader in current timeout window
	rf.receivedAppendEntriesInCurrentTimeout = false

	// Random number between 150 and 300 (ms) as specified on article
	timeout := time.Duration((rand.Intn(300-150) + 150)) * time.Millisecond

	fmt.Println("Waiting for AppendEntries", rf.me, timeout)

	time.Sleep(timeout)

	if rf.receivedAppendEntriesInCurrentTimeout {
		fmt.Println("Received a AppendEntries during timeout, starting another", rf.me)

		go rf.electionTimeout()
	} else {
		fmt.Println("Didn't received a AppendEntries during timeout, starting election", rf.me)

		go rf.election()
	}
}

func (rf *Raft) election() {
	rf.state = "candidate"
	rf.currentVotes = 0
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.persist()

	args := &RequestVoteArgs{}

	args.Term = rf.currentTerm
	// Votes for itself
	args.CandidateId = rf.me
	// Random values, not used on 2A case
	args.LastLogIndex = 0
	args.LastLogTerm = 0

	for peer := range rf.peers {
		// fmt.Println("Sending request to peer", rf.me, peer)
		go rf.sendRequestVote(peer, args, &RequestVoteReply{})
	}

	// Random number between 300 and 600 (ms)
	timeout := time.Duration((rand.Intn(600-300) + 300)) * time.Millisecond

	fmt.Println("Started election timeout", rf.me, timeout)

	time.Sleep(timeout)

	wonElection := (rf.currentVotes > len(rf.peers)/2) && rf.state == "candidate"

	fmt.Println("Election finished", rf.me, wonElection, rf.currentVotes, len(rf.peers)/2)

	if wonElection {
		fmt.Println("\033[32m", "Won election! Sending heartbeats", rf.me, rf.currentVotes)

		rf.state = "leader"
	} else {
		fmt.Println("\033[31m", "Lost election!", rf.me, rf.currentVotes)

		go rf.electionTimeout()
	}
}
