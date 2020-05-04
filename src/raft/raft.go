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
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

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
	Follower   int = 0
	Candidate   int = 1
	Leader     int = 2
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

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	raftState  int
	votedCount int

	heartBeatDone chan struct{}
	applyCh chan ApplyMsg
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	//if args.Term > rf.currentTerm{
	//	rf.currentTerm = args.Term
	//}qq
	rf.mu.Unlock()
	reply.VoteGranted = false
	//DPrintf("before voted reply is %v, me id is %d, votedFor is %d, candidateId is %d, current term is %v, " +
	//	"args term is %v", reply, rf.me, rf.votedFor, args.CandidateId, rf.currentTerm, args.LastLogTerm)
	if rf.checkVotedFor(args.CandidateId) || rf.checkVotedFor(-1){
		lastIndex := len(rf.log)-1
		lastLogTerm := rf.log[lastIndex].Term
		if (args.LastLogTerm > lastLogTerm) ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastIndex) {
			rf.mu.Lock()
			rf.votedFor = args.CandidateId
			rf.mu.Unlock()
			reply.VoteGranted = true
		}
	}
	//DPrintf("after voted reply is %v, me id is %d, votedFor is %d, candidateId is %d",
	//	reply, rf.me, rf.votedFor, args.CandidateId)

}

//
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
	rf.mu.Lock()
	isLeader = rf.raftState == Leader
	rf.mu.Unlock()
	if isLeader{
		isLeader = true
		rf.mu.Lock()
		entry := LogEntries{rf.currentTerm, command}
		rf.log = append(rf.log, entry)
		rf.nextIndex[rf.me]++
		rf.applyCh<-ApplyMsg{true, command,len(rf.log)-1}
		rf.mu.Unlock()
		term = rf.currentTerm
		index = len(rf.log)-1

		for i := 0; i < len(rf.peers); i++{
			if i != rf.me {
				successCount := 0
				hasUpdateCommit := false
				go func(server int) {
					rf.mu.Lock()
					args := AppendEntriesArgs{rf.currentTerm, rf.me, 0,0,
						[]LogEntries{{entry.Term, entry.Command}}, rf.commitIndex}
					rf.mu.Unlock()
					reply := AppendEntriesReply{0, false}
					//DPrintf("server next Index is %v", server)
					nextIndex := rf.nextIndex[server]-1
					leader := true
					for ;reply.Success == false && nextIndex >= 0 && leader; {
						rf.mu.Lock()
						args.PrevLogIndex = nextIndex
						//DPrintf("rf nextIndex is %v prelogindex %v log is %v", rf.nextIndex, args.PrevLogIndex, rf.log)
						args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
						rf.sendAppendEntries(server, &args, &reply)
						if reply.Term > rf.currentTerm {
							rf.raftState = Follower
							rf.currentTerm = reply.Term
							leader = false
						}
						rf.mu.Unlock()
						//if ok == false{
						//	DPrintf("-----------------------------send is failed")
						//	continue
						//}
						if reply.Success == false{
							rf.nextIndex[server] = rf.nextIndex[server]-1
						}else{
							successCount++
							if successCount * 2 > len(rf.peers) && !hasUpdateCommit{
								hasUpdateCommit = true
								rf.commitIndex = rf.commitIndex + 1
							}
							rf.matchIndex[server] = args.PrevLogIndex
							rf.mu.Lock()
							rf.nextIndex[server] = rf.nextIndex[server] + 1
							rf.mu.Unlock()
						}
						rf.mu.Lock()
						nextIndex = rf.nextIndex[server] - 1
						rf.mu.Unlock()
					}
				}(i)
			}
		}
	}

	//leader commit vs commitIndex
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	//first index is 1?
	rf.applyCh = applyCh
	rf.log = []LogEntries {LogEntries{0,0}}
	rf.applyCh<-ApplyMsg{true, 0,0}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.resetLeaderNextIndex()
	for i := 0; i < len(peers); i++ {
		//rf.nextIndex = append(rf.nextIndex, len(rf.log)-1)
		rf.matchIndex[i] =  0
	}
	rf.raftState = Follower

	rf.heartBeatDone = make(chan struct{}, 1)

	go func (){
		//leader election
		for{
			if !rf.checkRaftStatus(Leader){
				t := rand.Intn(150) + 200
				//t := getRandomTime(150, 350)
				heatBeat := time.NewTimer(time.Duration(t) * time.Millisecond)
				select {
					case <-heatBeat.C:
						if !rf.checkRaftStatus(Leader) {
							//DPrintf("me is %v, going to be elected", me)
							go leaderElection(rf, me, peers)
						}
					case <-rf.heartBeatDone:
						rf.setRaftStatus(Follower)
						rf.votedFor = -1
				}
			}
		}
	}()
	//go rf.waitForHeatBeat()

	//heartbeat
	go rf.sendHeartBeat()

	// rf.nextIndex = rf.leader
	// rf.matchIndex = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

//func (rf *Raft) waitForHeatBeat(){
//	for{
//		if !rf.checkRaftStatus(Leader){
//			t := rand.Intn(150) + 200
//			timer1 := time.NewTimer(time.Duration(t) * time.Millisecond)
//			select {
//			case <-timer1.C:
//				if !rf.checkRaftStatus(Leader) {
//					DPrintf("me is %v, going to be elected", rf.me)
//					go leaderElection(rf, rf.me, rf.peers)
//				}
//			case <-rf.heartBeatDone:
//				rf.setRaftStatus(Follower)
//				rf.votedFor = -1
//			}
//		}
//	}
//}

func (rf *Raft) sendHeartBeat(){
	for{
		time.Sleep(150* time.Millisecond)
		if rf.checkRaftStatus(Leader) {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					rf.mu.Lock()
					args := AppendEntriesArgs{rf.currentTerm, i, len(rf.log) - 2,
						rf.currentTerm - 1, []LogEntries{}, rf.commitIndex}
					rf.mu.Unlock()
					go func(server int) {
						reply := AppendEntriesReply{0, false}
						rf.sendAppendEntries(server, &args, &reply)
					rf.mu.Lock()
					if reply.Term > rf.currentTerm{
						rf.raftState = Follower
						rf.currentTerm = reply.Term
					}
					rf.mu.Unlock()
					}(i)
				}
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) checkRaftStatus(status int) bool{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.raftState == status
}

func (rf *Raft) setRaftStatus(status int){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.raftState = status
}

func (rf *Raft) checkVotedFor(votedFor int) bool{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor == votedFor
}

type LogEntries struct{
	Term int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term  int
	LeaderId  int
	PrevLogIndex int
	PrevLogTerm  int

	Entries []LogEntries
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func leaderElection(rf *Raft, me int, peers []*labrpc.ClientEnd){
	if !rf.checkVotedFor(-1){
		return
	}
	rf.mu.Lock()
	args := RequestVoteArgs{rf.currentTerm, rf.me, len(rf.log)-1,
		rf.log[len(rf.log)-1].Term}
	rf.raftState = Candidate
	rf.currentTerm++
	rf.votedCount = 1
	rf.votedFor = me
	rf.mu.Unlock()

	for i := 0; i < len(peers); i++ {
		if i != me {
			go func(server int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, &args, &reply)
				if ok {

					if reply.VoteGranted {
						rf.mu.Lock()
						rf.votedCount++
						if (rf.raftState == Candidate) && (rf.votedCount > len(peers)/2){
							DPrintf("****************leader is %v, currentTerm is %v", rf.me, rf.currentTerm)
							rf.raftState = Leader
							rf.resetLeaderNextIndex()
						}
						rf.mu.Unlock()
					}
					rf.mu.Lock()
					if reply.Term > rf.currentTerm{
						rf.raftState = Follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
					}
					rf.mu.Unlock()
				}
			}(i)
		}
	}
	rf.mu.Lock()
	rf.votedFor = -1
	rf.mu.Unlock()
}

//call in the server
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// to do
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm{
		rf.raftState = Follower
	}
	if rf.currentTerm < args.Term{
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.mu.Unlock()
	if len(args.Entries) == 0{
		rf.heartBeatDone<- struct{}{}
	}else {
		rf.mu.Lock()
		if args.Term < rf.currentTerm {
			DPrintf("case 1 node is %v", rf.me)
			reply.Success = false
		} else if (len(rf.log) > args.PrevLogIndex) && (rf.log[args.PrevLogIndex].Term == args.PrevLogTerm) {
			DPrintf("case 2")
			reply.Success = true
			//add new entries
			//rf.log = rf.log[0: args.PrevLogIndex+1]
			for i := 0; i < len(args.Entries); i++{
				rf.log = append(rf.log, args.Entries[i])
				rf.applyCh<-ApplyMsg{true, args.Entries[i].Command,len(rf.log)-1}
			}
			//DPrintf("rf log is %v node is %v", rf.log, rf.me)
			if args.LeaderCommit > rf.commitIndex{
				rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
			}
		} else {
			DPrintf("case 3")
			reply.Success = false
			//delete entries to do
			//DPrintf("rf.log len is %v, args.PrevLogIndex is %v node is %v", len(rf.log), args.PrevLogIndex, rf.me)
			rf.log = rf.log[0:args.PrevLogIndex+1]
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) resetLeaderNextIndex(){
	for i := 0; i < len(rf.nextIndex); i++{
		rf.nextIndex[i] = len(rf.log)
	}
}


func min(a int, b int) int{
	if a < b{
		return a
	}
	return b
}
//func getRandomTime(lowerThreshold int, upperThreshold int) int{
//	t := rand.Intn(upperThreshold-lowerThreshold) + lowerThreshold
//	return t
//}