package raftkv

import (
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	lastLeader  int
	cid         int64
	seq         int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.seq = 0
	ck.cid = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	getArgs := GetArgs{Key: key, Cid:ck.cid, Seq:ck.seq}
	reply := GetReply{}

	for {
		doneCh := make(chan bool, 1)
		go func() {
			//发送Get() RPC
			ok := ck.servers[ck.lastLeader].Call("KVServer.Get", &getArgs, &reply)
			doneCh <- ok
		}()

		select {
		case <-time.After(600 * time.Millisecond):
			DPrintf("clerk(%d) retry PutAppend after timeout\n", ck.cid)
			continue
		case ok := <- doneCh:
			//收到响应后，并且是leader返回的，那么说明这个命令已经执行了
			if ok && reply.WrongLeader == false {
				//请求序列号加1
				ck.seq++
				return reply.Value
			}
		}

		//换一个server重试
		ck.lastLeader++
		ck.lastLeader %= len(ck.servers)
	}

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, Op:op, Cid: ck.cid, Seq: ck.seq}
	reply := PutAppendReply{}
	for {
		doneCh := make(chan bool, 1)
		go func() {
			//发送Get() RPC
			ok := ck.servers[ck.lastLeader].Call("KVServer.PutAppend", &args, &reply)
			doneCh <- ok
		}()

		select {
		case <-time.After(600 * time.Millisecond):
			DPrintf("clerk(%d) retry PutAppend after timeout\n", ck.cid)
			continue
		case ok := <- doneCh:
			//收到响应后，并且是leader返回的，那么说明这个命令已经执行了
			if ok && reply.WrongLeader == false {
				//请求序列号加1
				ck.seq++
				return
			}
		}

		ck.lastLeader++
		ck.lastLeader %= len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
