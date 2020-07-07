package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

const (
	WrongLeader  bool = true
	IsLeader bool = false
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key string
	Cid int64
	Seq int
	Value string


}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	db      map[string]string
	latestReplies map[int64]*LatestReply
	notify map[int]chan struct{}
}

type LatestReply struct {
	Reply GetReply
	Seq int
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = WrongLeader
		reply.Err = ""
		return
	}

	// 防止重复请求
	kv.mu.Lock()
	if latestReply, ok := kv.latestReplies[args.Cid]; ok && args.Seq <= latestReply.Seq {
		reply.WrongLeader = IsLeader
		reply.Value = latestReply.Reply.Value
		reply.Err = latestReply.Reply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := Op{Operation:"Get", Key:args.Key, Cid:args.Cid, Seq:args.Seq}
	index, term, _ := kv.rf.Start(command)

	// 阻塞等待结果
	kv.mu.Lock()
	ch := make(chan struct{})
	kv.notify[index] = ch
	kv.mu.Unlock()
	select {
	case <-ch:
		curTerm, isLeader := kv.rf.GetState()
		DPrintf("%v got notify at index %v, isLeader = %v\n", kv.me, index, isLeader)
		if !isLeader || curTerm != term {
			reply.WrongLeader = WrongLeader
			reply.Err = ""
		} else {
			reply.WrongLeader = IsLeader
			kv.mu.Lock()
			if value, ok := kv.db[args.Key]; ok {
				reply.Value = value
				reply.Err = OK
			} else {
				reply.Err = ErrNoKey
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = WrongLeader
		reply.Err = ""
		return
	}

	kv.mu.Lock()
	if latestReply, ok := kv.latestReplies[args.Cid]; ok && args.Seq <= latestReply.Seq {
		reply.WrongLeader = IsLeader
		reply.Err = latestReply.Reply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := Op{Operation:args.Op, Key:args.Key, Value:args.Value, Cid:args.Cid, Seq:args.Seq}
	index, term, _ := kv.rf.Start(command)

	kv.mu.Lock()
	ch := make(chan struct{})
	kv.notify[index] = ch
	kv.mu.Unlock()

	select {
	case <-ch:
		curTerm, isLeader := kv.rf.GetState()
		if !isLeader || curTerm != term {
			reply.WrongLeader = WrongLeader
			reply.Err = NotLeader
		} else {
			reply.WrongLeader = IsLeader
			reply.Err = OK
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.latestReplies = make(map[int64]*LatestReply)
	kv.notify = make(map[int] chan struct{})
	go kv.applyDaemon()

	return kv
}


func (kv *KVServer) applyDaemon()  {
	for appliedEntry := range kv.applyCh {
		command := appliedEntry.Command.(Op)

		//DPrintf("get here")
		// 执行命令, 过滤已经执行过得命令
		kv.mu.Lock()
		if latestReply, ok := kv.latestReplies[command.Cid]; !ok || command.Seq > latestReply.Seq {
			switch command.Operation {
			case "Get":
				latestReply := LatestReply{Seq:command.Seq,}
				reply := GetReply{}
				if value, ok := kv.db[command.Key]; ok {
					reply.Value = value
				} else {
					reply.Err = ErrNoKey
				}
				latestReply.Reply = reply
				kv.latestReplies[command.Cid] = &latestReply
			case "Put":
				kv.db[command.Key] = command.Value
				latestReply := LatestReply{Seq:command.Seq}
				kv.latestReplies[command.Cid] = &latestReply
			case "Append":
				kv.db[command.Key] += command.Value
				latestReply := LatestReply{Seq:command.Seq}
				kv.latestReplies[command.Cid] = &latestReply
			default:
				panic("invalid command operation")
			}
		}

		DPrintf("%d applied index:%d, cmd:%v\n", kv.me, appliedEntry.CommandIndex, command)
		// 通知
		if ch, ok := kv.notify[appliedEntry.CommandIndex]; ok && ch != nil {
			DPrintf("%d notify index %d\n",kv.me, appliedEntry.CommandIndex)
			close(ch)
			delete(kv.notify, appliedEntry.CommandIndex)
		}
		kv.mu.Unlock()
	}
}