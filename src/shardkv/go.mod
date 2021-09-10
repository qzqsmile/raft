module shardkv

go 1.14

replace labgob => ../labgob

replace labrpc => ../labrpc

replace shardmaster => ../shardmaster

replace raft => ../raft

replace linearizability => ../linearizability

require (
	labgob v0.0.0-00010101000000-000000000000
	labrpc v0.0.0-00010101000000-000000000000
	linearizability v0.0.0-00010101000000-000000000000
	raft v0.0.0-00010101000000-000000000000
	shardmaster v0.0.0-00010101000000-000000000000
)
