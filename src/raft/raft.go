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
	"math"
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
	Term                  int
	LeaderId              int
	PrevLogIndex          int
	PrevLogTerm           int
	LeaderCommit          int
	LeaderCommitedLogTerm int
	Entries               []LogEntry
}

type AppendEntriesResponse struct {
	Term    int
	Success bool

	ConflictTerm       int
	ConflictFirstIndex int
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
	Command interface{}
}
type LogsState struct {
	logs          []LogEntry
	logsSize      int
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

	// Your data here (2A, 2B, 2C).
	raftState          RaftState
	syncLogFuncChannel chan int
	applyMsgSignalChan chan bool
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func (rf *Raft) updateHeartBeat() {
	rf.raftState.lastHeartBeat = curMill()
}

func (rf *Raft) AppendEntries(req *AppendEntriesRequest, res *AppendEntriesResponse) {

	rf.raftState.termLock.Lock()
	if rf.raftState.term < req.Term || (rf.raftState.term == req.Term && rf.raftState.role == 1) {
		rf.roleChange(req.Term, 0, rf.raftState.term < req.Term || rf.raftState.role != 1)
	} else if req.Term < rf.raftState.term {
		//fmt.Printf("ID: %d\n", rf.me)
		res.Term = rf.raftState.term
		res.Success = false
		rf.raftState.termLock.Unlock()
		return
	}

	rf.updateHeartBeat()

	logSize := len(rf.raftState.logsState.logs)
	//fmt.Printf("try agian1 %d\n", req.PrevLogIndex)
	//fmt.Printf("Sync Done %d %d\n", rf.raftState.logsState.commitedIndex, rf.raftState.logsState.logsSize)
	if len(req.Entries) == 0 {

		commitRes := false
		if rf.raftState.logsState.commitedIndex < req.LeaderCommit {
			var minIndex int = int(math.Min(float64(req.LeaderCommit), float64(logSize-1)))
			//fmt.Printf("comit\n")
			if rf.raftState.logsState.logs[minIndex].Term == req.LeaderCommitedLogTerm {
				rf.commitLog(minIndex)
				commitRes = true
			}
		}
		rf.raftState.termLock.Unlock()
		if commitRes {
			rf.applyMsgSignalChan <- true
		}
		res.Success = true
		res.Term = rf.raftState.term

		return
	}

	//fmt.Printf("try agian2\n")

	prevIndex := logSize - 1
	//server log index exceeds this follower
	//fmt.Printf("follower and leader logs: %d %d %d %d\n", rf.raftState.logsState.logs[len(rf.raftState.logsState.logs)-1].Term, req.PrevLogTerm, prevIndex, req.PrevLogIndex)
	if prevIndex < req.PrevLogIndex || rf.raftState.logsState.logs[req.PrevLogIndex].Term != req.PrevLogTerm {
		res.Success = false
		res.Term = rf.raftState.term
		//fmt.Printf("conflict!!!! %d\n", rf.raftState.lastHeartBeat)

		index := int(math.Min(float64(prevIndex), float64(req.PrevLogIndex)))
		if index < 1 {
			index = 1
		}
		res.ConflictTerm = rf.raftState.logsState.logs[index].Term
		for ; index > 1 && res.ConflictTerm == rf.raftState.logsState.logs[index-1].Term; index-- {
		}
		res.ConflictFirstIndex = index
		rf.raftState.termLock.Unlock()
		return
	}

	appenedEntriesFinalIndex := req.PrevLogIndex + len(req.Entries)
	beginIndex := req.PrevLogIndex + 1

	for beginIndex <= appenedEntriesFinalIndex && beginIndex < logSize {
		if rf.raftState.logsState.logs[beginIndex].Term == req.Entries[beginIndex-req.PrevLogIndex-1].Term {
			beginIndex++
		} else {
			break
		}
	}

	entriesStartIndex := beginIndex - req.PrevLogIndex - 1
	if entriesStartIndex < len(req.Entries) && beginIndex < logSize {
		rf.raftState.logsState.logs = rf.raftState.logsState.logs[:beginIndex]
	}
	//fmt.Printf("reqPrev: %d, original size: %d -> cutoff: %d， req.Entries: %d, id: %d, reqTerm: %d, leaderID: %d\n", req.PrevLogIndex, logSize, len(rf.raftState.logsState.logs), len(req.Entries), rf.me, req.Entries[len(req.Entries)-1].Term, req.LeaderId)
	for i := entriesStartIndex; i < len(req.Entries); i++ {
		rf.raftState.logsState.logs = append(rf.raftState.logsState.logs, req.Entries[i])
	}

	canCommit := false
	if req.LeaderCommit > rf.raftState.logsState.commitedIndex {
		minIdx := req.LeaderCommit
		lastEntryIdx := len(rf.raftState.logsState.logs) - 1
		if minIdx > lastEntryIdx {
			minIdx = lastEntryIdx
		}
		rf.commitLog(minIdx)
		canCommit = true
	}

	res.Term = rf.raftState.term
	rf.raftState.termLock.Unlock()
	res.Success = true
	if canCommit {
		rf.applyMsgSignalChan <- true
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

type ClosableChan struct {
	ch     chan int
	closed bool
	lock   sync.Mutex
}

func (this *ClosableChan) send(i int) {
	this.lock.Lock()
	defer this.lock.Unlock()
	if !this.closed {
		this.ch <- i
	}
}

func (this *ClosableChan) close() {
	this.lock.Lock()
	this.closed = true
	this.lock.Unlock()
}

func (this *ClosableChan) recv() int {
	return <-this.ch
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	//if rf.me == 1 {
	//
	//fmt.Printf("vote %d %d\n", rf.raftState.term, args.Term)
	//
	//}
	rf.raftState.termLock.Lock()
	defer rf.raftState.termLock.Unlock()

	if rf.raftState.term > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.raftState.term
		return
	} else if rf.raftState.term < args.Term {
		rf.roleChange(args.Term, 0, true)
	} else if rf.raftState.role == 1 {
		reply.VoteGranted = false
		reply.Term = rf.raftState.term
		return
	}

	reply.Term = rf.raftState.term
	curLastLogIndex := len(rf.raftState.logsState.logs) - 1
	curLastLogTerm := rf.raftState.logsState.logs[curLastLogIndex].Term

	if curLastLogTerm > args.LastLogTerm {
		reply.VoteGranted = false
	} else if args.LastLogTerm > curLastLogTerm || curLastLogIndex <= args.LastLogIndex {
		reply.VoteGranted = rf.raftState.canVote

		if rf.raftState.canVote {
			rf.updateHeartBeat()
		}
		rf.raftState.canVote = false

	}

	//if !reply.VoteGranted {
	//
	//	fmt.Printf("%v idTerm %d req.term %d for %d from id %d, req.lastLog %d, req.lastLogTerm %d. curLog %d, curTerm %d\n", f, rf.raftState.term, args.Term, args.CandidateId, rf.me, args.LastLogIndex, args.LastLogTerm, curLastLogIndex, curLastLogTerm)
	//
	//}

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

func (rf *Raft) sendAppendLogRequest(peerIndex int, targetCommitIndex int, useTerm int, ch *ClosableChan) {

	//againCount:=0
	sendLogIndex := -1
	for !rf.killed() {

		rf.raftState.termLock.Lock()
		//role := rf.raftState.role
		commitIndex := rf.raftState.logsState.commitedIndex
		sendLogIndex = rf.raftState.logsState.nextIndex[peerIndex]
		leaderCommittedLogTerm := rf.raftState.logsState.logs[commitIndex].Term
		logSize := len(rf.raftState.logsState.logs)
		if useTerm != rf.raftState.term || logSize-1 != targetCommitIndex {
			rf.raftState.termLock.Unlock()
			ch.send(0)
			return
		}
		prevLogTerm := rf.raftState.logsState.logs[sendLogIndex-1].Term
		entries := make([]LogEntry, targetCommitIndex-sendLogIndex+1)
		copy(entries, rf.raftState.logsState.logs[sendLogIndex:targetCommitIndex+1])
		rf.raftState.termLock.Unlock()

		response := AppendEntriesResponse{}
		request := AppendEntriesRequest{Term: useTerm, LeaderId: rf.me, PrevLogIndex: sendLogIndex - 1, PrevLogTerm: prevLogTerm, LeaderCommit: commitIndex, Entries: entries, LeaderCommitedLogTerm: leaderCommittedLogTerm}
		//fmt.Printf("prev %d", request.PrevLogIndex)
		ok := rf.peers[peerIndex].Call("Raft.AppendEntries", &request, &response)
		//fmt.Printf("sent\n")
		if !ok {
			//try again until success
			//fmt.Printf("again %d %d\n", rf.me, curMill())
			time.Sleep(time.Millisecond * 3)
			continue
		}

		//fmt.Printf("recv\n")

		//fmt.Printf("commit index: %v %d\n", response.Success, response.Term)

		if response.Term > useTerm {
			//become a follower.

			rf.raftState.termLock.Lock()
			if response.Term > rf.raftState.term {
				rf.roleChange(response.Term, 0, true)
			}
			rf.raftState.termLock.Unlock()
			ch.send(0)
			return
		}
		if response.Success {
			rf.raftState.termLock.Lock()
			if rf.raftState.logsState.matchIndex[peerIndex] <= targetCommitIndex {
				rf.raftState.logsState.matchIndex[peerIndex] = targetCommitIndex
			}
			isCurTerm := 1
			if useTerm != rf.raftState.term {
				isCurTerm = 0
			} else {
				if rf.raftState.logsState.nextIndex[peerIndex] <= targetCommitIndex {
					rf.raftState.logsState.nextIndex[peerIndex] = targetCommitIndex + 1
				}
			}
			rf.raftState.termLock.Unlock()
			ch.send(isCurTerm)
			return
		}

		rf.raftState.termLock.Lock()
		if rf.raftState.logsState.nextIndex[peerIndex] != sendLogIndex || rf.raftState.role != 2 {
			rf.raftState.termLock.Unlock()
			ch.send(0)
			return
		}

		nextSendIdx := 0
		for ; nextSendIdx+1 < sendLogIndex && rf.raftState.logsState.logs[nextSendIdx+1].Term < response.ConflictTerm; nextSendIdx++ {

		}

		sendLogIndex -= 1
		if sendLogIndex < 1 {
			sendLogIndex = 1
		}
		rf.raftState.logsState.nextIndex[peerIndex] = sendLogIndex
		rf.raftState.termLock.Unlock()

		//sendLogIndex -= 1
		//if sendLogIndex < 1 {
		//	sendLogIndex = 1
		//}

	}

	//fmt.Printf("killed\n")
	//ch.send(-1)

}

func (rf *Raft) commitLog(commitIndex int) {

	log := &rf.raftState.logsState.logs[commitIndex]
	if rf.raftState.role != 2 {
		rf.raftState.logsState.commitedIndex = commitIndex
	} else {
		if rf.raftState.term == log.Term {
			rf.raftState.logsState.commitedIndex = commitIndex
		}
	}
}

func (rf *Raft) synLog() {

	committIndex := -1
	canCommitCount := len(rf.peers)/2 + 1
	for !rf.killed() {

		committIndex = <-rf.syncLogFuncChannel

		rf.raftState.termLock.Lock()
		if rf.raftState.role != 2 || committIndex != len(rf.raftState.logsState.logs)-1 || rf.raftState.logsState.logs[committIndex].Term < rf.raftState.term {
			rf.raftState.termLock.Unlock()
			continue
		}

		useTerm := rf.raftState.term
		rf.raftState.termLock.Unlock()

		peerAccptSignalChan := ClosableChan{ch: make(chan int, len(rf.peers)), closed: false, lock: sync.Mutex{}}
		for i := 0; i < len(rf.peers); i++ {
			if rf.me == i {
				continue
			}
			go rf.sendAppendLogRequest(i, committIndex, useTerm, &peerAccptSignalChan)
		}

		accptCount := 1
	outer:
		for rf.raftState.role == 2 {

			accept := peerAccptSignalChan.recv()
			if accept <= 0 {
				peerAccptSignalChan.close()
				//fmt.Printf("end%d\n", rf.me)
				break outer
			}
			accptCount += 1

			if accptCount >= canCommitCount {
				rf.raftState.termLock.Lock()
				canCommit := false
				if rf.raftState.role == 2 && rf.raftState.logsState.commitedIndex < committIndex {
					rf.commitLog(committIndex)
					canCommit = true
				}
				rf.raftState.termLock.Unlock()
				if canCommit {
					rf.applyMsgSignalChan <- true
				}
				//fmt.Printf("commit == %d\n", committIndex)
				break outer
			}
		}

		//fmt.Printf("e c %d\n", committIndex)

		peerAccptSignalChan.close()

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

	//fmt.Printf("Start\n")
	index = len(rf.raftState.logsState.logs)
	term = rf.raftState.term
	isLeader = rf.raftState.role == 2

	if isLeader {
		//rf.raftState.logsState.logsLock.Lock()
		log := LogEntry{Term: term, Command: command}
		rf.raftState.logsState.logs = append(rf.raftState.logsState.logs, log)
		//fmt.Printf("Create Log: Term %d at term %d, index %d. Using index, %d\n", log.Term, rf.raftState.term, rf.raftState.logsState.logsSize, rf.raftState.logsState.logs[rf.raftState.logsState.logsSize].Term)
		//fmt.Printf("expire %d %d %d\n", rf.raftState.logsState.logs[rf.raftState.logsState.commitedIndex].Term, rf.raftState.term, rf.raftState.logsState.commitedIndex)
		//rf.raftState.logsState.logsLock.Unlock()
	}
	rf.raftState.termLock.Unlock()
	rf.syncLogFuncChannel <- index
	//fmt.Printf("enter %d\n", index)
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

func (rf *Raft) roleChange(term int, role int, canVote bool) {

	if term > rf.raftState.term {
		rf.raftState.electionTimeOut = int64(rand.Intn(400) + 150)
	}
	rf.updateTerm(term)
	rf.raftState.role = role
	rf.raftState.canVote = canVote
	rf.raftState.lastHeartBeat = curMill()
}

func (rf *Raft) sendVotesRequest(peer *labrpc.ClientEnd, channel chan int, args *RequestVoteArgs) {

	for !rf.killed() {

		//fmt.Printf("Vote")
		reply := RequestVoteReply{0, false}
		ok := peer.Call("Raft.RequestVote", args, &reply)
		if !ok {
			continue
		} else if reply.VoteGranted {
			channel <- 1
			return
		} else {
			vote := 0
			rf.raftState.termLock.Lock()
			if reply.Term > rf.raftState.term {
				rf.roleChange(reply.Term, 0, true)
				vote = -1
			}
			rf.raftState.termLock.Unlock()
			channel <- vote
			return
		}

		time.Sleep(2 * time.Millisecond)
	}

	channel <- -1
}

func (rf *Raft) heartbeatChildren(leaderTerm int) {
	//time.Sleep(5 * time.Millisecond)
	request := AppendEntriesRequest{
		Term:     leaderTerm,
		LeaderId: rf.me,
	}

	for !rf.killed() {

		//fmt.Printf("hearbeat %d %d %d %d\n ", rf.me, rf.raftState.term, rf.raftState.role, rf.raftState.lastHeartBeat)
		rf.raftState.termLock.Lock()
		if rf.raftState.term > leaderTerm {
			rf.raftState.termLock.Unlock()
			return
		}

		commit := rf.raftState.logsState.commitedIndex
		request.LeaderCommit = commit
		request.LeaderCommitedLogTerm = rf.raftState.logsState.logs[commit].Term
		rf.raftState.termLock.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			reply := AppendEntriesResponse{}
			go rf.peers[i].Call("Raft.AppendEntries", &request, &reply)
		}
		time.Sleep(time.Millisecond * 45)
	}

}

func (rf *Raft) askVotes(useTerm int, lastLogIndex int, lastLogTerm int) {

	var vote int = 1
	channel := make(chan int, len(rf.peers)+1)
	args := RequestVoteArgs{Term: useTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
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
			break
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
		rf.roleChange(useTerm, 2, false)
		//fmt.Printf("become leader %d\n", rf.raftState.term)
		go rf.heartbeatChildren(useTerm)
		nextIdx := len(rf.raftState.logsState.logs)
		for i := 0; i < len(rf.peers); i++ {
			rf.raftState.logsState.nextIndex[i] = nextIdx
			rf.raftState.logsState.matchIndex[i] = 0
		}
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
		time.Sleep(20 * time.Millisecond)
		rf.raftState.termLock.Lock()

		updateTerm := -1
		if rf.raftState.role != 2 {
			now := curMill()
			diff := now - rf.raftState.lastHeartBeat
			if diff > timeout {
				updateTerm = rf.raftState.term + 1
				rf.roleChange(updateTerm, 1, false)
				go rf.askVotes(rf.raftState.term, len(rf.raftState.logsState.logs)-1, rf.raftState.logsState.logs[len(rf.raftState.logsState.logs)-1].Term)
				//var newTimeOut int64 = 1

				//newTimeOut = int64(rand.Intn(450) + 150)

				//rf.raftState.electionTimeOut = newTimeOut
			}
		}
		rf.raftState.termLock.Unlock()

	}
}

func (rf *Raft) applyMsgCor(applyMsgChan chan ApplyMsg) {

	for {

		_ = <-rf.applyMsgSignalChan
		rf.raftState.termLock.Lock()
		end := rf.raftState.logsState.commitedIndex
		if end <= rf.raftState.logsState.lastApplied {
			rf.raftState.termLock.Unlock()
			continue
		}
		logs := make([]LogEntry, end-rf.raftState.logsState.lastApplied)
		copy(logs, rf.raftState.logsState.logs[rf.raftState.logsState.lastApplied+1:end+1])
		rf.raftState.termLock.Unlock()
		for i := 0; i < len(logs); i++ {
			//fmt.Printf("%d %d\n", logs[i].Term, rf.raftState.logsState.commitedIndex)

			msg := ApplyMsg{Command: logs[i].Command, CommandIndex: rf.raftState.logsState.lastApplied + 1, CommandValid: true}
			applyMsgChan <- msg
			rf.raftState.logsState.lastApplied++
		}

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
	rf.raftState = RaftState{canVote: true, role: 0, electionTimeOut: int64(rand.Intn(400) + 150), lastHeartBeat: -1, term: 0}
	// Your initialization code here (2A, 2B, 2C).
	//fmt.Printf("%d", len(peers))
	// initialize from state persisted before a crash
	rf.raftState.logsState.logs = make([]LogEntry, 1, 48)
	rf.raftState.logsState.nextIndex = make(map[int]int)
	rf.raftState.logsState.matchIndex = make(map[int]int)
	rf.applyMsgSignalChan = make(chan bool, 100)
	rf.syncLogFuncChannel = make(chan int, 50)
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.synLog()
	go rf.applyMsgCor(applyCh)

	return rf
}
