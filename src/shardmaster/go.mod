module shardmaster

go 1.14

replace labgob => ../labgob

replace labrpc => ../labrpc

replace raft => ../raft

require (
	labgob v0.0.0-00010101000000-000000000000
	labrpc v0.0.0-00010101000000-000000000000
	raft v0.0.0-00010101000000-000000000000
)
