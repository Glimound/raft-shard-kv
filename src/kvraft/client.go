package kvraft

import (
	"crypto/rand"
	"math/big"
	"raft-shard-kv/src/labrpc"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId     int64
	requestId    int64
	recentLeader int
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
	ck.clientId = nrand()
	ck.requestId = 0
	DPrintf(dClerk, "Clerk initiated, clientId: %d", ck.clientId)
	return ck
}

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
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
		CommandId: nrand(),
	}
	reply := GetReply{}

	for {
		server := ck.recentLeader
		for i := 0; i < len(ck.servers); i++ {
			index := (server + i) % len(ck.servers)
			DPrintf(dClerk, "Clerk %d sending Get RPC to S%d", ck.clientId, index)
			ok := ck.servers[index].Call("KVServer.Get", &args, &reply)
			if !ok || reply.Err == ErrWrongLeader {
				DPrintf(dClerk, "Get Wrong leader reply from S%d", index)
				continue
			}
			if reply.Err == OK || reply.Err == ErrNoKey {
				result := reply.Value
				DPrintf(dClerk, "Get OK reply from S%d, value: %s", index, result)
				ck.recentLeader = index
				ck.requestId++
				return result
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
		CommandId: nrand(),
		Value:     value,
		Op:        op,
	}
	reply := PutAppendReply{}

	for {
		server := ck.recentLeader
		for i := 0; i < len(ck.servers); i++ {
			index := (server + i) % len(ck.servers)
			DPrintf(dClerk, "Clerk %d sending %s RPC to S%d", ck.clientId, args.Op, index)
			ok := ck.servers[index].Call("KVServer.PutAppend", &args, &reply)
			if !ok || reply.Err == ErrWrongLeader {
				DPrintf(dClerk, "%s Wrong leader reply from S%d", args.Op, index)
				continue
			}
			DPrintf(dClerk, "%s OK reply from S%d, key: %s, value: %s", args.Op, index, args.Key, args.Value)
			ck.recentLeader = index
			ck.requestId++
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
