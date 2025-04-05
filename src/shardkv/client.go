package shardkv

import (
	"crypto/rand"
	"math/big"
	"time"

	"raft-shard-kv/src/labrpc"
	"raft-shard-kv/src/shardctrler"
)

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm           *shardctrler.Clerk
	config       shardctrler.Config
	make_end     func(string) *labrpc.ClientEnd
	clientId     int64
	requestId    int64
	recentLeader map[int]int // GID -> leader index
}

func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.clientId = nrand()
	ck.requestId = 0
	ck.recentLeader = make(map[int]int)
	DPrintf(dClerk, "Clerk initiated, clientId: %d", ck.clientId)
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
		CommandId: nrand(),
	}
	reply := GetReply{}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			var server int
			if v, ok := ck.recentLeader[gid]; ok {
				server = v
			} else {
				server = 0
			}
			for i := 0; i < len(servers); i++ {
				index := (server + i) % len(servers)
				srv := ck.make_end(servers[index])
				DPrintf(dClerk, "Clerk %d sending Get RPC to G%d, S%d", ck.clientId, gid, index)
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					result := reply.Value
					DPrintf(dClerk, "Get OK reply from G%d, S%d, value: %s", gid, index, result)
					ck.recentLeader[gid] = index
					ck.requestId++
					return result
				}
				if ok && (reply.Err == ErrWrongGroup) {
					DPrintf(dClerk, "Get Wrong group reply from G%d, S%d", gid, index)
					break
				}
				if !ok || reply.Err == ErrWrongLeader {
					DPrintf(dClerk, "Get Wrong leader reply from G%d, S%d", gid, index)
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
		CommandId: nrand(),
	}
	reply := PutAppendReply{}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			var server int
			if v, ok := ck.recentLeader[gid]; ok {
				server = v
			} else {
				server = 0
			}
			for i := 0; i < len(servers); i++ {
				index := (server + i) % len(servers)
				srv := ck.make_end(servers[index])
				DPrintf(dClerk, "Clerk %d sending %s RPC to G%d, S%d", ck.clientId, op, gid, index)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && (reply.Err == OK) {
					DPrintf(dClerk, "%s OK reply from G%d, S%d, key: %s, value: %s", op, gid, index, args.Key, args.Value)
					ck.recentLeader[gid] = index
					ck.requestId++
					return
				}
				if ok && (reply.Err == ErrWrongGroup) {
					DPrintf(dClerk, "%s Wrong group reply from G%d, S%d", op, gid, index)
					break
				}
				if !ok || reply.Err == ErrWrongLeader {
					DPrintf(dClerk, "%s Wrong leader reply from G%d, S%d", op, gid, index)
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
