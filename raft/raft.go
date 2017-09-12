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
	"labrpc"
	"sync"

	rand "math/rand"
	"time"
)

// import "fmt"

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

type Log struct {
	Index   int
	Term    int
	Command interface{}
}

const (
	notVotedForAnyone = -1
)

const (
	Follower  = iota
	Candidate = iota
	Leader    = iota
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	currentTerm                   int
	votedFor                      int
	lastApplied                   int
	commitIndex                   int
	log                           []Log
	nextIndex                     []int
	matchIndex                    []int
	electionTimeout               int
	requestElectionSuccessChannel chan bool
	heartBeat                     chan bool
	StatePhase                    int
	logsToCommit                  chan bool
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.StatePhase == Leader {
		isleader = true
	} else {
		isleader = false
	}
	// DPrintf("total info", rf.me, rf.currentTerm, rf.StatePhase)
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int // is indexed from 1
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) CheckLogAsUpToDate(args *RequestVoteArgs) bool {
	if len(rf.log) == 0 {
		return true
	} else {
		if rf.log[len(rf.log)-1].Term == args.LastLogTerm {
			if args.LastLogIndex >= len(rf.log)-1 {
				return true
			}
		} else if args.LastLogTerm > rf.log[len(rf.log)-1].Term {
			return true
		}
	}
	return false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// stepdown
	if args.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.StatePhase = Follower
		rf.votedFor = notVotedForAnyone
		rf.mu.Unlock()
	}

	// otherwise
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	DPrintf("what are the stats", rf.me, rf.votedFor, len(rf.log))
	if rf.votedFor == notVotedForAnyone || rf.votedFor == args.CandidateId {
		if len(rf.log) == 0 || rf.CheckLogAsUpToDate(args) {
			reply.VoteGranted = true
			rf.mu.Lock()
			rf.votedFor = args.CandidateId
			rf.mu.Unlock()
		} else {
			reply.VoteGranted = false
		}
	}
	DPrintf("lets see how the voting goes", rf.me, rf.votedFor, args.CandidateId, reply.VoteGranted, len(rf.peers))
	return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) checkLogEntryAtPrevLogIndex(args *AppendEntriesArgs) bool {
	// fmt.Println("what stats are we comapring", args.PrevLogIndex, len(rf.log))
	if len(rf.log)-1 < args.PrevLogIndex {
		return false
	}
	if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		// fmt.Println("we are returning the correct thing")
		return true
	}
	return false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// stepdown
	// fmt.Println("start sending heartbeat")
	if args.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.StatePhase = Follower
		rf.votedFor = notVotedForAnyone
		rf.currentTerm = args.Term
		rf.mu.Unlock()
	}

	// main logic
	reply.Term = rf.currentTerm
	reply.Success = true
	if args.Term < rf.currentTerm {
		// fmt.Println("term error")
		reply.Success = false
		return
	}
	// send heartbeat
	// fmt.Println("we have heartbeat from", rf.me, args.LeaderId)
	rf.heartBeat <- true
	// fmt.Println("we have sent the heartbeat from", rf.me, args.LeaderId)
	if !rf.checkLogEntryAtPrevLogIndex(args) {
		reply.Success = false
		return
	}
	// append new entries
	// if len(args.Entries) == 0 {
	// 	return
	// }
	// put a check for more conditions?
	go func() {
		rf.mu.Lock()
		// fmt.Println("========logs current status", rf.me, rf.log, args.PrevLogIndex, rf.log[:args.PrevLogIndex], args.Entries, args.LeaderCommit)
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
		commitIndex := rf.commitIndex
		rf.mu.Unlock()
		if args.LeaderCommit > commitIndex {
			rf.mu.Lock()
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
			rf.mu.Unlock()
			// fmt.Println("==== start by commiting for", rf.me, rf.commitIndex)
			rf.logsToCommit <- true
			// fmt.Println("=== send the signal", rf.me, rf.commitIndex)
		}
		// fmt.Println("$$$$$ status is", rf.me, rf.commitIndex, rf.log)
	}()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// fmt.Println("we are getting the heartbeat")
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// fmt.Println("heartbeat reply %v", reply)
	DPrintf("from heartbeat we can say", ok, reply)
	if ok {
		if reply.Term > rf.currentTerm {
			rf.mu.Lock()
			rf.StatePhase = Follower
			rf.votedFor = notVotedForAnyone
			rf.mu.Unlock()
			return ok
		}
		if reply.Success {
			rf.mu.Lock()
			rf.nextIndex[server] = min(rf.nextIndex[server]+1, len(rf.log))
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			rf.nextIndex[server] = rf.nextIndex[server] - 1
			rf.mu.Unlock()
		}
	}
	return ok
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
	term = rf.currentTerm

	if rf.StatePhase == Leader {
		isLeader = true
		newLogEntry := Log{Index: len(rf.log), Term: rf.currentTerm, Command: command}
		rf.mu.Lock()
		rf.log = append(rf.log, newLogEntry)
		index = len(rf.log) - 1
		rf.mu.Unlock()
	} else {
		isLeader = false
	}
	if isLeader {
		// fmt.Printf("#########we are adding new entry at %d %d %v", index, term, isLeader)
	}
	return index, term, isLeader
}

func (rf *Raft) follower() {
	// fmt.Printf("i am a follower (%d) (%d)\n", rf.me, rf.currentTerm, rf.log, rf.commitIndex, rf.lastApplied)
	electionTimerTick := time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond)
	select {
	case <-electionTimerTick.C:
		// change state to candidate
		DPrintf("I am becoming candidate:", rf.me)
		rf.mu.Lock()
		rf.StatePhase = Candidate
		rf.mu.Unlock()
		return
	case <-rf.heartBeat:
		DPrintf("I am still a follower", rf.me)
		rf.mu.Lock()
		rf.StatePhase = Follower
		rf.votedFor = notVotedForAnyone
		rf.mu.Unlock()
		return
	}
}

// increment currentTerm
// voteForSelf
// reset election timer
// send requestVote RPC to all other servers
func (rf *Raft) askForVotes() {
	DPrintf("REELection...", rf.me)
	rf.mu.Lock()
	rf.currentTerm = rf.currentTerm + 1
	rf.mu.Unlock()

	rf.mu.Lock()
	rf.votedFor = rf.me
	rf.mu.Unlock()

	// rf.resetElectionTimeout()
	status := make(chan bool)
	voteReply := make(chan *RequestVoteReply)
	args := new(RequestVoteArgs)
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogTerm = rf.log[len(rf.log)-1].Term
	args.LastLogIndex = len(rf.log) - 1
	go func() {
		for index := range rf.peers {
			if index != rf.me {
				currIndex := index
				go func() {
					reply := new(RequestVoteReply)
					DPrintf("requesting votes from", rf.me, currIndex)
					voteStatus := rf.sendRequestVote(currIndex, args, reply)
					DPrintf("we have recieved some status of the reply", voteStatus, currIndex, rf.me)
					status <- voteStatus
					if voteStatus {
						voteReply <- reply
					}
				}()
			}
		}
	}()

	countVotesForSelf := 0
	for index := range rf.peers {
		if index != rf.me {
			if <-status {
				voteReplyForIndex := <-voteReply
				if voteReplyForIndex.VoteGranted {
					countVotesForSelf++
				}
				if voteReplyForIndex.Term > rf.currentTerm {
					rf.mu.Lock()
					rf.currentTerm = voteReplyForIndex.Term
					rf.StatePhase = Follower
					rf.votedFor = notVotedForAnyone
					rf.mu.Unlock()
					return
				}
			}
		} else {
			countVotesForSelf++
		}
	}

	DPrintf("votes recieved", countVotesForSelf, rf.me)
	if 2*countVotesForSelf > len(rf.peers) {
		rf.requestElectionSuccessChannel <- true
	}
}

func (rf *Raft) candidate() {
	// do everything thats required over here
	// fmt.Printf("i am a candidate: (%d) (%d)\n", rf.me, rf.currentTerm)
	electionTimerTick := time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond)
	go rf.askForVotes()
	select {
	case <-electionTimerTick.C:
		return
	case <-rf.heartBeat:
		rf.mu.Lock()
		rf.StatePhase = Follower
		rf.votedFor = notVotedForAnyone
		rf.mu.Unlock()
		return
	case <-rf.requestElectionSuccessChannel:
		DPrintf("i am becoming leader", rf.me)
		rf.mu.Lock()
		rf.StatePhase = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for index := range rf.peers {
			rf.nextIndex[index] = len(rf.log)
			rf.matchIndex[index] = 0
		}
		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) SendAppendEntriesRPC() {
	rf.mu.Lock()
	commitIndex := rf.commitIndex
	// fmt.Println("what are the states of the nextIndex %v %d", rf.nextIndex, rf.commitIndex)
	// fmt.Println("whats the state for matchIndexmatchIndex %v", rf.matchIndex)
	for indexToCommit := rf.commitIndex; indexToCommit < len(rf.log); indexToCommit++ {
		countServers := 1
		for index := range rf.peers {
			// // fmt.Printf("I am peer (%d) and checking (%d)\n", index, rf.matchIndex[index])
			if rf.matchIndex[index] >= indexToCommit && rf.me != index && rf.log[indexToCommit].Term == rf.currentTerm {
				countServers++
			}
		}
		if 2*countServers > len(rf.peers) {
			commitIndex = indexToCommit
		}
	}
	// fmt.Println("whats the commit index now", rf.me, commitIndex)
	rf.commitIndex = commitIndex
	rf.logsToCommit <- true
	rf.mu.Unlock()

	// send heartbeat
	// send queries from here
	// // fmt.Println("lets start sending heatbeat")
	for index := range rf.peers {
		if index != rf.me && rf.StatePhase == Leader {
			// send queries from here
			// fmt.Println("stats %d %d\n", len(rf.log), rf.nextIndex[index])
			if len(rf.log) > rf.nextIndex[index] {
				entries := make([]Log, len(rf.log[rf.nextIndex[index]:]))
				copy(entries, rf.log[rf.nextIndex[index]:])
				// fmt.Println("====apppending entries are %v===", entries)
				appendEntriesArgs := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[index] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[index]-1].Term,
					LeaderCommit: rf.commitIndex,
					Entries:      entries}
				appendEntriesReply := new(AppendEntriesReply)
				go rf.sendAppendEntries(index, &appendEntriesArgs, appendEntriesReply)
			} else {
				// fmt.Println("===we are in the else===", len(rf.log), rf.nextIndex[index])
				// panic("we fucked up big time")
				appendEntriesArgs := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[index] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[index]-1].Term,
					LeaderCommit: rf.commitIndex}
				appendEntriesReply := new(AppendEntriesReply)
				go rf.sendAppendEntries(index, &appendEntriesArgs, appendEntriesReply)
			}
		}
	}
}

func (rf *Raft) leader() {
	// fmt.Println("LEADER", rf.me, rf.commitIndex, rf.lastApplied, rf.log)
	heartBeatTimer := time.NewTimer(time.Duration(150) * time.Millisecond)
	go rf.SendAppendEntriesRPC()
	// go rf.SendAppendEntriesRPC()
	select {
	case <-heartBeatTimer.C:
		return
	}
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

func getElectionTimeout() int {
	electionTimeout := rand.Int() % 600
	if electionTimeout < 300 {
		electionTimeout = electionTimeout + 300
	}
	return electionTimeout
}

func (rf *Raft) resetElectionTimeout() {
	// rf.electionTimerTick = time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond).C
}

func (rf *Raft) resetHeartBeatTimer() {
	// rf.electionTimerTick = time.NewTimer(time.Duration(200) * time.Millisecond).C
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.electionTimeout = getElectionTimeout()
	DPrintf("election timeout", rf.electionTimeout)
	rf.votedFor = notVotedForAnyone
	rf.lastApplied = 0
	rf.currentTerm = 0
	rf.StatePhase = Follower
	rf.heartBeat = make(chan bool)
	rf.requestElectionSuccessChannel = make(chan bool)
	rf.logsToCommit = make(chan bool)
	rf.log = append(rf.log, Log{Term: 0})
	rf.commitIndex = 0
	// if rf.me == 0 {
	// 	rf.electionTimeout = 800
	// } else if rf.me == 1 {
	// 	rf.electionTimeout = 1000
	// } else {
	// 	rf.electionTimeout = 1200
	// }

	// Your initialization code here (2A, 2B, 2C).
	go func() {
		for {
			if rf.StatePhase == Leader {
				// fmt.Printf("i am a leader (%d) (%d)\n", rf.me, rf.currentTerm)
				// DPrintf("i am a leader", rf.me)
			}
			// DPrintf("what is the term", rf.me, rf.StatePhase)
			if rf.StatePhase == Follower {
				rf.follower()
			} else if rf.StatePhase == Candidate {
				rf.heartBeat = make(chan bool)
				rf.candidate()
			} else {
				rf.leader()
			}
		}
	}()

	// we need to apply the command to the state machine
	go func() {
		for {
			select {
			case <-rf.logsToCommit:
				// fmt.Printf("&&&&&& commiting logs for %d %d %d logLength: %d\n", rf.me, rf.commitIndex, rf.lastApplied, len(rf.log))
				rf.mu.Lock()
				for index := rf.lastApplied + 1; index <= rf.commitIndex; index++ {
					if index > len(rf.log)-1 {
						panic("severe fuck up happning here")
					}
					channelMessage := ApplyMsg{Index: index, Command: rf.log[index].Command}
					applyCh <- channelMessage
					rf.lastApplied = index
				}
				// fmt.Println("&&&&& We have updated the lastApplied", rf.me, rf.lastApplied, rf.commitIndex)
				rf.mu.Unlock()
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
