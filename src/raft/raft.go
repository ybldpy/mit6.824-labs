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
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type AppendEntriesRequest struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesResponse struct {
	Term    int
	Success bool

	CommitIndex int
	LogTerms    []int
}

type RaftState struct {
	term     int
	termLock sync.Mutex
	//0-follower 1-candicate 2-leader
	role            int
	electionTimeOut int64
	lastHeartBeat   int64
	canVote         bool
	logsState       LogsState
}

type LogEntry struct {
	Term    int
	Idx     int
	Command interface{}
}
type LogsState struct {
	logs          []LogEntry
	logsLock      sync.Mutex
	nextIndex     map[int]int
	nextIndexLock sync.Mutex
	matchIndex    map[int]int
	commitedIndex int
	lastApplied   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	id        int
	// Your data here (2A, 2B, 2C).
	raftState          RaftState
	syncLogFuncChannel chan int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func (rf *Raft) updateHeartBeat() {

	rf.raftState.lastHeartBeat = curMill()
}

func (rf *Raft) AppendEntries(req *AppendEntriesRequest, res *AppendEntriesResponse) {

	rf.raftState.termLock.Lock()
	if len(req.Entries) == 0 {
		if req.Term >= rf.raftState.term {
			rf.raftState.role = 0
			rf.raftState.term = req.Term
			rf.raftState.canVote = true
			rf.updateHeartBeat()
		}
		rf.raftState.termLock.Unlock()
	}

	if rf.raftState.role == 2 {
		rf.raftState.termLock.Unlock()
		res.Term = req.Term
		res.Success = true
		return
	}

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).

	rf.raftState.termLock.Lock()
	defer rf.raftState.termLock.Unlock()

	//fmt.Printf("current term: %d, killed: %v, id: %d, role: %d\n", rf.raftState.term, rf.killed(), rf.me, rf.raftState.role)
	term = rf.raftState.term
	isleader = rf.raftState.role == 2
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.raftState.termLock.Lock()
	defer rf.raftState.termLock.Unlock()

	if rf.raftState.term > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.raftState.term
		return
	}
	if rf.raftState.term < args.Term {
		rf.updateTerm(args.Term)
		rf.roleChange(0)
		rf.raftState.canVote = true

	}

	//fmt.Printf("%d voted to %d: %v\n", rf.me, args.CandidateId, rf.raftState.canVote)
	reply.VoteGranted = rf.raftState.canVote
	rf.raftState.canVote = false
	reply.Term = rf.raftState.term
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendLogRequest(peerIndex int, peerAccptSignalChan chan int) {

	for {

		rf.raftState.termLock.Lock()
		term := rf.raftState.term
		role := rf.raftState.role
		commitIndex := rf.raftState.logsState.commitedIndex
		rf.raftState.termLock.Unlock()
		//exit early. If term increase and role changes when executing following, the peer will reject our request so it won't cause any problem.
		if role != 2 {
			peerAccptSignalChan <- 0
			return
		}

		//rf.raftState.logsState.nextIndexLock.Lock()
		sendLogIndex := rf.raftState.logsState.nextIndex[peerIndex]
		//rf.raftState.logsState.nextIndexLock.Unlock()

		rf.raftState.logsState.logsLock.Lock()
		prevLogTerm := rf.raftState.logsState.logs[sendLogIndex].Term
		entries := make([]LogEntry, len(rf.raftState.logsState.logs)-sendLogIndex)
		for i := 0; i < len(entries); i++ {
			entries[i] = rf.raftState.logsState.logs[sendLogIndex+i]
		}
		rf.raftState.logsState.logsLock.Unlock()

		response := AppendEntriesResponse{}
		request := AppendEntriesRequest{Term: term, LeaderId: rf.me, PrevLogIndex: sendLogIndex - 1, PrevLogTerm: prevLogTerm, LeaderCommit: commitIndex}
		ok := rf.peers[peerIndex].Call("Raft.AppendEntries", &request, &response)
		if !ok {
			//try again until success
			continue
		}
		if response.Term > term {
			//become a follower.
			rf.raftState.termLock.Lock()
			if response.Term > rf.raftState.term {
				rf.updateTerm(response.Term)
				rf.updateHeartBeat()
				rf.roleChange(0)
			}
			rf.raftState.termLock.Unlock()
			return
		}
		if response.Success {
			peerAccptSignalChan <- 1
			return
		}

		rf.raftState.logsState.logsLock.Lock()
		i := response.CommitIndex
		for ; i < len(rf.raftState.logsState.logs); i++ {
			if rf.raftState.logsState.logs[i].Term != response.LogTerms[i] {
				break
			}
		}
		rf.raftState.logsState.nextIndex[peerIndex] = i
		rf.raftState.logsState.logsLock.Unlock()

	}

}

func (rf *Raft) commitLog(commitIndex int) {
	log := &rf.raftState.logsState.logs[commitIndex]
	if rf.raftState.role != 2 {
		//todo: do commit
		rf.raftState.logsState.commitedIndex = commitIndex
	} else {
		if rf.raftState.term == log.Term {
			//todo: do commit

			rf.raftState.logsState.commitedIndex = commitIndex
		}

	}
}

func (rf *Raft) synLog() {

	for true {
		committIndex := <-rf.syncLogFuncChannel
		peerAccptSignalChan := make(chan int)
		for i := 0; i < len(rf.peers); i++ {
			if rf.me == i {
				continue
			}
			go rf.sendAppendLogRequest(i, peerAccptSignalChan)
		}

		accptCount := 1
	outer:
		for rf.raftState.role == 2 {
			select {
			case accept := <-peerAccptSignalChan:
				if accept <= 0 {
					break outer
				}
				accptCount += accept
				if accptCount > len(rf.peers) {
					rf.raftState.termLock.Lock()
					rf.commitLog(committIndex)
					rf.raftState.termLock.Unlock()
					accptCount = -1
				}
			}
		}

		close(peerAccptSignalChan)

	}

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	rf.raftState.termLock.Lock()
	index = len(rf.raftState.logsState.logs)
	term = rf.raftState.term
	isLeader = rf.raftState.role == 2
	if isLeader {
		rf.raftState.logsState.logsLock.Lock()
		log := LogEntry{Term: term, Idx: len(rf.raftState.logsState.logs), Command: command}
		rf.raftState.logsState.logs = append(rf.raftState.logsState.logs, log)
		rf.syncLogFuncChannel <- len(rf.raftState.logsState.logs) - 1
		rf.raftState.logsState.logsLock.Unlock()
	}
	rf.raftState.termLock.Unlock()
	return index, term, isLeader
}

func (rf *Raft) updateTerm(term int) {

	rf.raftState.term = term

}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) roleChange(role int) {
	rf.raftState.role = role
	rf.raftState.lastHeartBeat = curMill()
}

func (rf *Raft) sendVotesRequest(peer *labrpc.ClientEnd, channel chan int, args *RequestVoteArgs) {

	reply := RequestVoteReply{0, false}
	ok := peer.Call("Raft.RequestVote", args, &reply)
	if !ok {
		channel <- 0
	} else if reply.VoteGranted {
		channel <- 1
	} else {
		vote := 0
		rf.raftState.termLock.Lock()
		if reply.Term > rf.raftState.term {
			rf.updateTerm(reply.Term)
			rf.roleChange(0)
			rf.raftState.canVote = true
			vote = -1
		}
		rf.raftState.termLock.Unlock()
		channel <- vote
	}
}

func (rf *Raft) heartbeatChildren(leaderTerm int) {
	request := AppendEntriesRequest{
		Term:     leaderTerm,
		LeaderId: rf.me,
	}
	reply := AppendEntriesResponse{}

	for rf.raftState.term == leaderTerm {
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.peers[i].Call("Raft.AppendEntries", &request, &reply)
		}
		//fmt.Printf("leaderId: %d\n", rf.me)
		time.Sleep(time.Millisecond * 80)
	}
}

func (rf *Raft) askVotes(useTerm int) {

	var vote int = 1
	channel := make(chan int)
	args := RequestVoteArgs{Term: useTerm, CandidateId: rf.me}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendVotesRequest(rf.peers[i], channel, &args)
	}

	winVotes := len(rf.peers)/2 + 1

	for rf.raftState.term == useTerm && rf.raftState.role == 1 {
		res := <-channel
		// 收到投票
		if res == -1 {
			return
		}
		vote += res
		if vote >= winVotes {
			break // 如果获得足够票数，退出
		}
	}

	rf.raftState.termLock.Lock()
	defer rf.raftState.termLock.Unlock()
	if rf.raftState.role == 0 || rf.raftState.term > useTerm {
		return
	} else if vote >= winVotes {

		//fmt.Printf("Node %d win leader with %d votes at term %d\n", rf.me, vote, rf.raftState.term)
		rf.raftState.role = 2
		go rf.heartbeatChildren(useTerm)
	} else {
		rf.raftState.role = 0
	}

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		timeout := rf.raftState.electionTimeOut
		time.Sleep(time.Duration(timeout))
		rf.raftState.termLock.Lock()

		updateTerm := -1
		if rf.raftState.role != 2 {
			now := curMill()
			diff := now - rf.raftState.lastHeartBeat
			if diff > timeout {
				updateTerm = rf.raftState.term + 1
				rf.updateTerm(updateTerm)
				rf.roleChange(1)
				rf.raftState.canVote = false
				go rf.askVotes(rf.raftState.term)
			}
		}
		//fmt.Printf("term: %d", rf.raftState.term)
		rf.raftState.termLock.Unlock()

	}

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.raftState = RaftState{canVote: true, role: 0, electionTimeOut: int64(rand.Intn(130) + 300), lastHeartBeat: -1, term: 0}
	// Your initialization code here (2A, 2B, 2C).
	//fmt.Printf("%d", len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.synLog()

	return rf
}
