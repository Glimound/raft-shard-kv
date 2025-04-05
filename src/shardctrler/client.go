package shardctrler

import "raft-shard-kv/src/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers      []*labrpc.ClientEnd
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
	ck.clientId = nrand()
	ck.requestId = 0
	DPrintf(dClerk, "Clerk initiated, clientId: %d", ck.clientId)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{}
	args.Num = num
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId
	args.CommandId = nrand()
	reply := QueryReply{}

	for {
		server := ck.recentLeader
		for i := 0; i < len(ck.servers); i++ {
			index := (server + i) % len(ck.servers)
			DPrintf(dClerk, "Clerk %d sending Query RPC to S%d", ck.clientId, index)
			ok := ck.servers[index].Call("ShardCtrler.Query", &args, &reply)
			if !ok || reply.Err == ErrWrongLeader {
				DPrintf(dClerk, "Query Wrong leader reply from S%d", index)
				continue
			}
			if reply.Err == OK {
				DPrintf(dClerk, "Query OK reply from S%d", index)
				ck.recentLeader = index
				ck.requestId++
				return reply.Config
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{}
	args.Servers = servers
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId
	args.CommandId = nrand()
	reply := JoinReply{}

	for {
		server := ck.recentLeader
		for i := 0; i < len(ck.servers); i++ {
			index := (server + i) % len(ck.servers)
			DPrintf(dClerk, "Clerk %d sending Join RPC to S%d", ck.clientId, index)
			ok := ck.servers[index].Call("ShardCtrler.Join", &args, &reply)
			if !ok || reply.Err == ErrWrongLeader {
				DPrintf(dClerk, "Join Wrong leader reply from S%d", index)
				continue
			}
			if reply.Err == OK {
				DPrintf(dClerk, "Join OK reply from S%d", index)
				ck.recentLeader = index
				ck.requestId++
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{}
	args.GIDs = gids
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId
	args.CommandId = nrand()
	reply := LeaveReply{}

	for {
		server := ck.recentLeader
		for i := 0; i < len(ck.servers); i++ {
			index := (server + i) % len(ck.servers)
			DPrintf(dClerk, "Clerk %d sending Leave RPC to S%d", ck.clientId, index)
			ok := ck.servers[index].Call("ShardCtrler.Leave", &args, &reply)
			if !ok || reply.Err == ErrWrongLeader {
				DPrintf(dClerk, "Leave Wrong leader reply from S%d", index)
				continue
			}
			if reply.Err == OK {
				DPrintf(dClerk, "Leave OK reply from S%d", index)
				ck.recentLeader = index
				ck.requestId++
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{}
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId
	args.CommandId = nrand()
	reply := MoveReply{}

	for {
		server := ck.recentLeader
		for i := 0; i < len(ck.servers); i++ {
			index := (server + i) % len(ck.servers)
			DPrintf(dClerk, "Clerk %d sending Move RPC to S%d", ck.clientId, index)
			ok := ck.servers[index].Call("ShardCtrler.Move", &args, &reply)
			if !ok || reply.Err == ErrWrongLeader {
				DPrintf(dClerk, "Move Wrong leader reply from S%d", index)
				continue
			}
			if reply.Err == OK {
				DPrintf(dClerk, "Move OK reply from S%d", index)
				ck.recentLeader = index
				ck.requestId++
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}
