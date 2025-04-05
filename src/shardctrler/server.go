package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"raft-shard-kv/src/labgob"
	"raft-shard-kv/src/labrpc"
	"raft-shard-kv/src/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	dupMap        map[int64]int64
	notifyChanMap map[int64]chan Notification
	dead          int32

	configs []Config // indexed by config num
}

type Notification struct {
	err   Err
	value string
}

type Op struct {
	Operation int
	Servers   map[int][]string
	GIDs      []int
	Shard     int
	GID       int
	Num       int
	ClientId  int64
	RequestId int64
	CommandId int64
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	if sc.killed() {
		reply.WrongLeader = true
		return
	}

	op := Op{
		Operation: OpJoin,
		Servers:   args.Servers,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		CommandId: args.CommandId,
	}

	sc.mu.Lock()
	_, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	DPrintf(dServer, "S%d receive valid Join RPC from C%d, R%d", sc.me, args.ClientId, args.RequestId)

	notifyChan := make(chan Notification, 1)
	sc.notifyChanMap[op.CommandId] = notifyChan
	sc.mu.Unlock()

	// when leader changed (term changed), it should redirect immediately
	go sc.termDetector(term, op.CommandId)

	select {
	case notification := <-notifyChan:
		reply.Err = notification.err
		if notification.err == ErrWrongLeader {
			reply.WrongLeader = true
		}
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true
	}

	sc.mu.Lock()
	delete(sc.notifyChanMap, op.CommandId)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	if sc.killed() {
		reply.WrongLeader = true
		return
	}

	op := Op{
		Operation: OpLeave,
		GIDs:      args.GIDs,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		CommandId: args.CommandId,
	}

	sc.mu.Lock()
	_, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	DPrintf(dServer, "S%d receive valid Leave RPC from C%d, R%d", sc.me, args.ClientId, args.RequestId)

	notifyChan := make(chan Notification, 1)
	sc.notifyChanMap[op.CommandId] = notifyChan
	sc.mu.Unlock()

	// when leader changed (term changed), it should redirect immediately
	go sc.termDetector(term, op.CommandId)

	select {
	case notification := <-notifyChan:
		reply.Err = notification.err
		if notification.err == ErrWrongLeader {
			reply.WrongLeader = true
		}
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true
	}

	sc.mu.Lock()
	delete(sc.notifyChanMap, op.CommandId)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	if sc.killed() {
		reply.WrongLeader = true
		return
	}

	op := Op{
		Operation: OpMove,
		Shard:     args.Shard,
		GID:       args.GID,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		CommandId: args.CommandId,
	}

	sc.mu.Lock()
	_, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	DPrintf(dServer, "S%d receive valid Move RPC from C%d, R%d", sc.me, args.ClientId, args.RequestId)

	notifyChan := make(chan Notification, 1)
	sc.notifyChanMap[op.CommandId] = notifyChan
	sc.mu.Unlock()

	// when leader changed (term changed), it should redirect immediately
	go sc.termDetector(term, op.CommandId)

	select {
	case notification := <-notifyChan:
		reply.Err = notification.err
		if notification.err == ErrWrongLeader {
			reply.WrongLeader = true
		}
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true
	}

	sc.mu.Lock()
	delete(sc.notifyChanMap, op.CommandId)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	if sc.killed() {
		reply.WrongLeader = true
		return
	}

	op := Op{
		Operation: OpQuery,
		Num:       args.Num,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		CommandId: args.CommandId,
	}

	sc.mu.Lock()
	_, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	DPrintf(dServer, "S%d receive valid Query RPC from C%d, R%d", sc.me, args.ClientId, args.RequestId)

	notifyChan := make(chan Notification, 1)
	sc.notifyChanMap[op.CommandId] = notifyChan
	sc.mu.Unlock()

	// when leader changed (term changed), it should redirect immediately
	go sc.termDetector(term, op.CommandId)

	select {
	case notification := <-notifyChan:
		reply.Err = notification.err
		sc.mu.Lock()
		if args.Num == -1 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		sc.mu.Unlock()
		if notification.err == ErrWrongLeader {
			reply.WrongLeader = true
		}
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true
	}

	sc.mu.Lock()
	delete(sc.notifyChanMap, op.CommandId)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		msg := <-sc.applyCh
		if !msg.CommandValid {
			continue
		}

		notification := Notification{}
		notification.err = OK
		op := msg.Command.(Op)

		sc.mu.Lock()
		lastRequestId, exist := sc.dupMap[op.ClientId]
		duplicate := exist && lastRequestId >= op.RequestId
		if !duplicate {
			switch op.Operation {
			case OpJoin:
				// 克隆当前最新的配置并增加版本号
				newConfig := cloneConfig(sc.configs[len(sc.configs)-1])
				newConfig.Num += 1

				// 将新的服务器组添加到配置中
				for k, v := range op.Servers {
					newConfig.Groups[k] = make([]string, len(v))
					copy(newConfig.Groups[k], v)
				}

				// 构建从GID到其管理的分片索引的映射
				shardMapping := make(map[int][]int) // GID to shard index
				for shardIndex, gid := range newConfig.Shards {
					shardMapping[gid] = append(shardMapping[gid], shardIndex)
				}

				// 收集所有当前的GID
				gids := make([]int, 0)
				for gid := range shardMapping {
					gids = append(gids, gid)
				}

				// 按照拥有的分片数量从多到少排序GID
				sort.Slice(gids, func(i, j int) bool {
					// 保证gids的顺序一致，len(slices)相同时，按gids顺序升序排序
					if len(shardMapping[gids[i]]) == len(shardMapping[gids[j]]) {
						return gids[i] < gids[j]
					}
					return len(shardMapping[gids[i]]) > len(shardMapping[gids[j]])
				})

				// 创建一个缓冲区用于临时存储需要重新分配的分片
				buffer := make([]int, 0)

				// 特殊情况：如果只有GID为0的组(表示未分配)，将所有分片放入缓冲区
				if len(gids) == 1 && gids[0] == 0 {
					gids = make([]int, 0)                       // 清空GID列表
					buffer = append(buffer, shardMapping[0]...) // 所有分片进入缓冲区等待分配
				}

				// 将新加入的GID添加到列表中
				newGids := make([]int, 0, len(op.Servers))
				for gid := range op.Servers {
					newGids = append(newGids, gid)
				}
				sort.Ints(newGids)
				gids = append(gids, newGids...)

				// 计算每个组应该拥有的平均分片数
				n := len(newConfig.Shards) / len(gids) // 每个组至少应有的分片数
				m := len(newConfig.Shards) % len(gids) // 前m个组可以多分配一个分片

				// 遍历所有GID，重新分配分片以实现负载均衡
				for i := 0; i < len(gids); i++ {
					// 计算当前GID应拥有的分片数
					shardNum := n
					if i < m {
						shardNum++ // 前m个组多分配一个分片
					}

					gid := gids[i]
					shards := shardMapping[gid]
					// 计算当前拥有的分片数与应有的分片数之差
					diff := len(shards) - shardNum

					if diff > 0 {
						// 如果拥有过多的分片，将多余的分片放入缓冲区
						buffer = append(buffer, shards[len(shards)-diff:]...)
						shardMapping[gid] = shards[:len(shards)-diff]
					} else if diff < 0 {
						// 如果分片不足，从缓冲区获取需要的分片
						// 注意：新加入的GID通常会走这个分支
						shardMapping[gid] = buffer[len(buffer)+diff:] // 从缓冲区尾部获取-diff个分片

						// 更新配置，将这些分片的所有权分配给当前GID
						for _, shard := range buffer[len(buffer)+diff:] {
							newConfig.Shards[shard] = gids[i]
						}

						// 更新缓冲区，移除已分配的分片
						buffer = buffer[:len(buffer)+diff]
					}
				}

				// 将新配置添加到配置列表中
				sc.configs = append(sc.configs, newConfig)
				DPrintf(dServer, "S%d done Join operation, log I%d, R%d, new config: %v",
					sc.me, msg.CommandIndex, op.RequestId, newConfig)
			case OpLeave:
				// 克隆当前最新的配置并增加版本号
				newConfig := cloneConfig(sc.configs[len(sc.configs)-1])
				newConfig.Num += 1

				// 构建从GID到其管理的分片索引的映射
				shardMapping := make(map[int][]int) // GID to shard index
				for shardIndex, gid := range newConfig.Shards {
					shardMapping[gid] = append(shardMapping[gid], shardIndex)
				}

				// 创建一个缓冲区用于临时存储需要重新分配的分片
				buffer := make([]int, 0)

				// 从配置和映射中移除离开的组，并将其分片加入缓冲区
				leaveGids := op.GIDs
				for _, gid := range leaveGids {
					// 将要离开的组的分片添加到缓冲区
					if shards, exists := shardMapping[gid]; exists {
						buffer = append(buffer, shards...)
						delete(shardMapping, gid)
					}
					// 从配置中删除该组
					delete(newConfig.Groups, gid)
				}

				// 收集剩余的所有GID
				gids := make([]int, 0)
				for gid := range newConfig.Groups {
					gids = append(gids, gid)
				}
				sort.Ints(gids)

				// 特殊情况处理：如果所有组都离开了
				if len(gids) == 0 {
					// 将所有分片设为未分配状态(GID=0)
					for i := range newConfig.Shards {
						newConfig.Shards[i] = 0
					}
				} else {
					// 按照拥有的分片数量从多到少排序GID
					sort.Slice(gids, func(i, j int) bool {
						if len(shardMapping[gids[i]]) == len(shardMapping[gids[j]]) {
							return gids[i] < gids[j]
						}
						return len(shardMapping[gids[i]]) > len(shardMapping[gids[j]])
					})

					// 计算每个组应该拥有的平均分片数
					n := len(newConfig.Shards) / len(gids) // 每个组至少应有的分片数
					m := len(newConfig.Shards) % len(gids) // 前m个组可以多分配一个分片

					// 首先，从所有组中收集额外的分片到缓冲区
					for i := 0; i < len(gids); i++ {
						gid := gids[i]
						shards := shardMapping[gid]

						// 计算当前GID应拥有的分片数
						shardNum := n
						if i < m {
							shardNum++ // 前m个组多分配一个分片
						}

						// 如果当前组拥有过多的分片，将多余的放入缓冲区
						if len(shards) > shardNum {
							extraCount := len(shards) - shardNum
							buffer = append(buffer, shards[len(shards)-extraCount:]...)
							shardMapping[gid] = shards[:len(shards)-extraCount]
						} else if len(shards) < shardNum && len(buffer) > 0 {
							neededCount := shardNum - len(shards)
							// 从缓冲区分配分片给当前组
							newShards := buffer[len(buffer)-neededCount:]
							for _, shard := range newShards {
								newConfig.Shards[shard] = gid
							}
							// 更新缓冲区
							buffer = buffer[:len(buffer)-neededCount]
						}
					}
				}

				// 将新配置添加到配置列表中
				sc.configs = append(sc.configs, newConfig)
				DPrintf(dServer, "S%d done Leave operation, log I%d, R%d, new config: %v",
					sc.me, msg.CommandIndex, op.RequestId, newConfig)
			case OpMove:
				newConfig := cloneConfig(sc.configs[len(sc.configs)-1])
				newConfig.Num += 1
				newConfig.Shards[op.Shard] = op.GID
				sc.configs = append(sc.configs, newConfig)
				DPrintf(dServer, "S%d done Move operation, log I%d, R%d, new config: %v",
					sc.me, msg.CommandIndex, op.RequestId, newConfig)
			case OpQuery:

			}
			sc.dupMap[op.ClientId] = op.RequestId
		} else {
			DPrintf(dServer, "S%d find duplicate log I%d, R%d <= %d, operation: %d", sc.me, msg.CommandIndex,
				op.RequestId, lastRequestId, op.Operation)
		}
		notifyChan, ok := sc.notifyChanMap[op.CommandId]
		if ok {
			// 必须使用非阻塞发送，因为RPC处理器可能已经超时
			select {
			case notifyChan <- notification:
			default:
			}
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) termDetector(copyTerm int, commandId int64) {
	for !sc.killed() {
		sc.mu.Lock()
		notifyChan, exist := sc.notifyChanMap[commandId]
		if !exist {
			sc.mu.Unlock()
			return
		}
		if currentTerm, _ := sc.rf.GetState(); currentTerm != copyTerm {
			DPrintf(dServer, "Detect term changed, current T%d != T%d", currentTerm, copyTerm)
			notification := Notification{}
			notification.err = ErrWrongLeader
			select {
			case notifyChan <- notification:
			default:
			}
		}
		sc.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func cloneConfig(oldConfig Config) Config {
	newConfig := Config{}
	newConfig.Num = oldConfig.Num
	newConfig.Shards = oldConfig.Shards
	newConfig.Groups = make(map[int][]string)
	for k, v := range oldConfig.Groups {
		newConfig.Groups[k] = make([]string, len(v))
		copy(newConfig.Groups[k], v)
	}
	return newConfig
}

func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.rf.SetDebugType("shardctrler")

	sc.dupMap = make(map[int64]int64)
	sc.notifyChanMap = make(map[int64]chan Notification)

	go sc.applier()
	DPrintf(dServer, "SC Server S%d initiated", me)

	return sc
}
