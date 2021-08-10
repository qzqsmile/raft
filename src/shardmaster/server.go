package shardmaster

import (
	"log"
	"raft"
	"time"
)
import "labrpc"
import "sync"
import "labgob"

const Debug = 0

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
	config Config
}


func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.WrongLeader = true
	if _, isLeader := sm.rf.GetState(); isLeader{
		reply.WrongLeader = false
		config := Config{
			Num:    0,
			Shards: [10]int{},
			Groups: args.Servers,
		}
		op := Op{
			config: config,
		}
		index, term, _ := sm.rf.Start(op)
		timer := time.NewTimer(1000 * time.Millisecond)
		ch := make(chan struct{})
		sm.notify[index] = ch
		select {
			case <-timer.C:
				reply.Err = "Failed to agree"
			case <-sm.applyCh:

		}
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.WrongLeader = true
	if _, isLeader := sm.rf.GetState(); isLeader{
		reply.WrongLeader = false

	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.WrongLeader = true
	if _, isLeader := sm.rf.GetState(); isLeader{
		reply.WrongLeader = false

	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	reply.WrongLeader = true
	if _, isLeader := sm.rf.GetState(); isLeader{
		reply.WrongLeader = false
		if len(sm.configs) > 0{
			if args.Num >= 0 && args.Num < len(sm.configs){
				reply.Config = sm.configs[args.Num]
			}else{
				reply.Config = sm.configs[len(sm.configs)-1]
			}
		}else{
			reply.Err = "No config"
		}
	}
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.notify = make(map[int] chan struct{})

	// Your code here.
	go sm.applyDaemon()

	return sm
}


func (sm *ShardMaster) applyDaemon()  {
	for appliedEntry := range sm.applyCh {
		sm.mu.Lock()
		if appliedEntry.CommandValid {
			command := appliedEntry.Command.(Op)
			sm.configs = append(sm.configs, command.config)
			if ch, ok := sm.notify[appliedEntry.CommandIndex]; ok && ch != nil {
				DPrintf("%d notify index %d\n", sm.me, appliedEntry.CommandIndex)
				close(ch)
				delete(sm.notify, appliedEntry.CommandIndex)
			}
		}
		sm.mu.Unlock()
	}
}
