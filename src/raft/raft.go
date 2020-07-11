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
	"bytes"
	"labgob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	Follower  int = 0
	Candidate int = 1
	Leader    int = 2
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
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntries

	commitIndex   int
	lastApplied   int
	electionTimer *time.Timer

	nextIndex  []int
	matchIndex []int

	raftState  int
	votedCount int

	heartBeatDone chan struct{}
	applyCh       chan ApplyMsg

	killElectionEventLoop chan struct{}
	killHeartBeatLoop     chan struct{}

	lastIncludedIndex int
	lastIncludedTerm int
	//heartBeatDone chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.raftState == Leader

}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntries
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		DPrintf("deocde error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
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
	LastLogIndex int
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	//DPrintf("before voted reply is %v, me id is %d, votedFor is %d, candidateId is %d, current term is %v, " +
	//	"args term is %v", reply, rf.me, rf.votedFor, args.CandidateId, rf.currentTerm, args.LastLogTerm)

	if rf.currentTerm < args.Term {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.raftState = Follower
		rf.resetTimer()
	}
	if rf.votedFor == args.CandidateId || rf.votedFor == -1 {
		lastIndex := len(rf.log) - 1
		lastLogTerm := rf.log[lastIndex].Term
		if (args.LastLogTerm > lastLogTerm) ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastIndex) {
			rf.votedFor = args.CandidateId
			rf.raftState = Follower
			reply.VoteGranted = true
			rf.resetTimer()
		}
	}
	rf.persist()
	//if reply.VoteGranted {
	//	DPrintf("in voting, my node is %v vote to %v, my log is %v isVoted %v, args is %v",
	//		rf.me, args.CandidateId, rf.log, reply.VoteGranted, args)
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
// if you're having trouble ge tting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	//rf.readPersist(rf.persister.ReadRaftState())

	rf.mu.Lock()
	if isLeader = rf.raftState == Leader; isLeader {
		term = rf.currentTerm
		index = len(rf.log)
		rf.log = append(rf.log, LogEntries{rf.currentTerm, command})
		rf.matchIndex[rf.me] = len(rf.log) - 1
	}
	rf.persist()
	rf.mu.Unlock()
	//leader commit vs commitIndex
	return index, term, isLeader
}

func (rf *Raft) updateCfgLogs(applyMsgs []ApplyMsg) {
	for _, v := range applyMsgs {
		rf.applyCh <- v
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
	rf.killElectionEventLoop <- struct{}{}
	rf.killHeartBeatLoop <- struct{}{}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.electionTimer = time.NewTimer(0)
	//first index is 1?
	rf.applyCh = applyCh
	rf.log = []LogEntries{LogEntries{0, 0}}

	//rf.applyCh <- ApplyMsg{true, 0, 0}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.resetLeaderNextIndex()
	rf.resetLeaderMatchIndex()

	rf.raftState = Follower
	rf.killHeartBeatLoop = make(chan struct{}, 1)
	rf.killElectionEventLoop = make(chan struct{}, 1)

	//may be need modification later
	rf.lastIncludedIndex = 0

	go rf.leaderElectionEventLoop()
	go rf.heartbeatEventLoop()

	// initialize from state persisted before a crash
	rf.readPersist(rf.persister.ReadRaftState())

	return rf
}

func (rf *Raft) leaderElectionEventLoop() {
	for {
		rf.mu.Lock()
		rf.resetTimer()
		rf.mu.Unlock()
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.raftState != Leader {
				//DPrintf("me is %v, going to be elected", me)
				go rf.leaderElection()
			}
			rf.mu.Unlock()
		case <-rf.killElectionEventLoop:
			return
		}
	}
}

func (rf *Raft) heartbeatEventLoop() {
	for {
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-timer.C:
			rf.mu.Lock()
			if rf.raftState == Leader {
				go rf.sendHeartbeat()
			}
			rf.mu.Unlock()
		case <-rf.killHeartBeatLoop:
			return
		}
	}
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			args := AppendEntriesArgs{rf.currentTerm, rf.me, len(rf.log) - 1,
				rf.log[len(rf.log)-1].Term, []LogEntries{}, rf.commitIndex}
			go func(server int, entriesArgs AppendEntriesArgs) {
				reply := AppendEntriesReply{}
				if ok := rf.sendAppendEntries(server, &entriesArgs, &reply); ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.raftState = Follower
						rf.votedFor = -1
						rf.persist()
					}
					//leader send its log to follower if false
					if reply.Success == false {
						go func(server int) {
							rf.mu.Lock()
							nextIndex := rf.nextIndex[server]
							islonger := rf.lastIncludedIndex + len(rf.log)-1 >= rf.nextIndex[server]
							rf.mu.Unlock()
							DPrintf("islonger is %v, nextIndex is %v", islonger, nextIndex)
							for ; islonger && nextIndex >= 1; {
								rf.mu.Lock()
								if rf.raftState != Leader {
									rf.mu.Unlock()
									break
								}

								if rf.nextIndex[server] <= rf.lastIncludedIndex {
									go rf.sendSnapshot(server)
									rf.mu.Unlock()
									return
								}

								PrevLogIndex := rf.nextIndex[server] - rf.lastIncludedIndex - 1
								var entries []LogEntries
								for i := PrevLogIndex+1; i < len(rf.log); i++ {
									entries = append(entries, rf.log[i])
								}
								args := AppendEntriesArgs{rf.currentTerm, rf.me, PrevLogIndex,
									rf.log[PrevLogIndex].Term, entries, rf.commitIndex}

								reply := AppendEntriesReply{0, false, 0, 0}
								rf.mu.Unlock()
								if ok := rf.sendAppendEntries(server, &args, &reply); ok {
									rf.mu.Lock()
									//set follower state
									if reply.Term > rf.currentTerm {
										rf.raftState = Follower
										rf.currentTerm = reply.Term
										rf.persist()
										rf.mu.Unlock()
										break
									}
									if reply.Success == false {
										DPrintf("relply conflict index is %v， args is %v", reply.ConflictIndex, args)
										rf.nextIndex[server] = reply.ConflictIndex
										nextIndex = rf.nextIndex[server]
									} else {
										rf.matchIndex[server] = args.PrevLogIndex + len(entries)
										rf.nextIndex[server] = args.PrevLogIndex + len(entries)+1
										go rf.leaderCommit()
									}
									islonger = len(rf.log)-1 >= rf.nextIndex[server]
									rf.mu.Unlock()
								} else {
									DPrintf("failed")
									break
								}
							}
						}(server)
					}
				}
			}(i, args)
		}
	}
}

func (rf *Raft) leaderCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	n := len(rf.log) - 1
	var committedMsgs []ApplyMsg

	for ; n > 0 && rf.log[n].Term == rf.currentTerm; {
		c := 0
		for i := 0; i < len(rf.matchIndex); i++ {
			if rf.matchIndex[i] >= n {
				c += 1
			}
		}
		if c*2 > len(rf.peers) {
			for j := rf.commitIndex + 1; j <= n; j++ {
				committedMsgs = append(committedMsgs, ApplyMsg{true,
					rf.log[j].Command, j})
			}
			go rf.updateCfgLogs(committedMsgs)
			rf.commitIndex = n
			break
		}
		n--
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type LogEntries struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int

	Entries      []LogEntries
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm int
}

func (rf *Raft) leaderElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
	rf.raftState = Candidate
	rf.votedCount = 1
	rf.votedFor = rf.me
	args := RequestVoteArgs{rf.currentTerm, rf.me, len(rf.log) - 1,
		rf.log[len(rf.log)-1].Term}

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				reply := RequestVoteReply{}
				if ok := rf.sendRequestVote(server, &args, &reply); ok {
					rf.mu.Lock()
					if reply.VoteGranted && args.Term == rf.currentTerm{
						rf.votedCount++
						if (rf.raftState == Candidate) && (rf.votedCount > len(rf.peers)/2) {
							rf.raftState = Leader
							//leader never commit before term index, so remove aren't able update index
							//rf.log = rf.log[0:rf.commitIndex+1]
							DPrintf("****************leader is %v, currentTerm is %v commit index is %v log is %v" +
								"nextIndex is %v", rf.me, rf.currentTerm, rf.commitIndex, rf.log, rf.nextIndex)
							rf.resetLeaderNextIndex()
							rf.resetLeaderMatchIndex()
						}
					}
					if reply.Term > rf.currentTerm {
						rf.raftState = Follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
					}
					rf.mu.Unlock()
				}
			}(i)
		}
	}
}

//call in the server
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if rf.currentTerm <= args.Term {
		rf.resetTimer()
	}
	if rf.currentTerm < args.Term {
		rf.raftState = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	//rf currentTerm is more update
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	DPrintf("me is %v log is %v", rf.me, rf.log)
	if len(rf.log)-1 < args.PrevLogIndex ||
		(rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		if len(rf.log)-1 < args.PrevLogIndex {
			reply.ConflictIndex = len(rf.log)
		} else{
			//faster moving by term, not index
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			for i := args.PrevLogIndex; i >= 0; i--{
				if rf.log[i].Term == rf.log[args.PrevLogIndex].Term{
					reply.ConflictIndex = i
				}else{
					break
				}
			}
		}
		return
	}

	//when hit this branch mean in PrevLogIndex all commits are matched with the leader
	//delete entries not match the PreLogIndex

	//if len(rf.log) >= args.PrevLogIndex + len(args.Entries){
	//	isMatch := true
	//	for i := 0; i < len(args.Entries); i++ {
	//		if args.Entries[i] != rf.log[i+args.PrevLogIndex+1] {
	//			isMatch = false
	//		}
	//	}
	//	if isMatch == false{
	//		rf.log = rf.log[0 : args.PrevLogIndex+1]
	//		rf.log = append(rf.log, args.Entries...)
	//	}
	//}else {
	//	rf.log = rf.log[0 : args.PrevLogIndex+1]
	//	rf.log = append(rf.log, args.Entries...)
	//}

	rf.log = rf.log[0 : args.PrevLogIndex+1]
	reply.Success = true

	rf.log = append(rf.log, args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		commitIndex := min(args.LeaderCommit, len(rf.log)-1)
		var Msgs []ApplyMsg
		for i := rf.commitIndex + 1; i <= commitIndex; i++ {
			Msgs = append(Msgs, ApplyMsg{true,
				rf.log[i].Command, i})
		}
		go rf.updateCfgLogs(Msgs)
		rf.commitIndex = commitIndex
	}
	rf.persist()
}

func (rf *Raft) resetLeaderNextIndex() {
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.log)+rf.lastIncludedIndex
	}
}

func (rf *Raft) resetLeaderMatchIndex() {
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) resetTimer() {
	t := rand.Intn(150) + 250
	rf.electionTimer.Reset(time.Duration(t) * time.Millisecond)
}


func(rf *Raft) SaveSnapshot(lastIndex int, snapShot []byte){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//if lastIndex > rf.lastIncludedIndex {
		startIndex := lastIndex-rf.lastIncludedIndex
		rf.lastIncludedIndex = lastIndex
		rf.lastIncludedTerm = rf.log[lastIndex].Term
		rf.log = append([]LogEntries{}, rf.log[startIndex:]...)
		data := rf.getPersistedData()
		rf.persister.SaveStateAndSnapshot(data, snapShot)
	//}
}

func(rf *Raft) getPersistedData() []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
}

func (rf *Raft) sendSnapshot(server int) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}


type InstallSnapshotArgs struct {
	Term	int
	LeaderId     int
	LastIncludedIndex int
	LastIncludedTerm  int

	Offset	int
	Data	[]byte
	Done bool
}

type InstallSnapshotReply struct {
	Term    int
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply InstallSnapshotReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm{
		return
	}
	var snapshot []byte
	if args.Offset == 0{
		r := bytes.NewBuffer(args.Data)
		d := labgob.NewDecoder(r)
		d.Decode(&snapshot)
	}
	if args.Done == false{
		return
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	if len(rf.log) > args.LastIncludedIndex && rf.log[args.LastIncludedIndex].Term == args.Term{
		rf.log = append([]LogEntries{}, rf.log[args.LastIncludedIndex+1:]...)
		return
	}

	rf.log = []LogEntries{}
}