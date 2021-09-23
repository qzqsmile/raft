package shardmaster

import (
	"fmt"
	"labgob"
	"labrpc"
	"raft"
	"sync"
)

const Debug = 1

const (
	WrongLeader  bool = true
	IsLeader bool = false
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	notify map[int]chan struct{}

	configs []Config // indexed by config num
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your data here.
	Operation string
	MasterConfig Config
	// Cid string
	// Seq string
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); isLeader{
		reply.WrongLeader = false
		// Num&Shards will change later
		newConfig := Config{
			Num:    len(sm.configs),
			Groups: args.Servers,
		}
		// command := Op{Operation:"Join", MasterConfig: newConfig, Cid:args.Cid, Seq:args.Seq}
		command := Op{Operation:"Join", MasterConfig: newConfig}
		

		index, term, _ := sm.rf.Start(command)
		//
		sm.mu.Lock()
		ch := make(chan struct{})
		sm.notify[index] = ch
		sm.mu.Unlock()
		
		<-ch
		fmt.Println(sm.configs)
		curTerm, isLeader := sm.rf.GetState()
		if !isLeader || curTerm != term {
			reply.WrongLeader = WrongLeader
			reply.Err = ""
		} else {
			reply.WrongLeader = IsLeader
		}

	}else{
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DPrintf("In Query\n")
	if _, isLeader := sm.rf.GetState(); isLeader{
		reply.WrongLeader = false
		// Num&Shards will change later
		sm.mu.Lock()

		if len(sm.configs) > 0{
			if args.Num == -1{
				reply.Config = sm.configs[len(sm.configs)-1]
			}else{
				if args.Num < len(sm.configs){
					reply.Config = sm.configs[args.Num]
				}else{
					reply.Err = "Num Exeed the length of configs"
				}
			}
		}else{
			reply.Err = "Empty Configs"
		}
		sm.mu.Unlock()
	}else{
		// DPrintf("In Query 2\n")

		reply.WrongLeader = true
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

func (sm *ShardMaster) applyDaemon(){
	for appliedEntry := range sm.applyCh {
		sm.mu.Lock()
		
		if appliedEntry.CommandValid {
			command := appliedEntry.Command.(Op)
			switch command.Operation {
			case "Join":
				sm.configs = append(sm.configs,command.MasterConfig)
			case "Query":
				//
			case "Leave":
				//
			default:
				panic("invalid command operation")
			}
			}

			if ch, ok := sm.notify[appliedEntry.CommandIndex]; ok && ch != nil {
				DPrintf("%d notify index %d\n", sm.me, appliedEntry.CommandIndex)
				close(ch)
				delete(sm.notify, appliedEntry.CommandIndex)
			}
			sm.mu.Unlock()
		}
}
