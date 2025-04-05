package kvraft

import (
	"bytes"
	"raft-shard-kv/src/labgob"
	"raft-shard-kv/src/labrpc"
	"raft-shard-kv/src/raft"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	Operation int
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
	CommandId int64
}

type Notification struct {
	err   Err
	value string
}

type Snapshot struct {
	Store  map[string]string
	Index  int
	DupMap map[int64]int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	store     map[string]string
	dupMap    map[int64]int64
	persister *raft.Persister

	// 为什么不使用set存储id并判断是否重复？
	// 最后只会有一个server回应RPC，并在回应后将set中对应的entry删除
	// 此时其余的server没法在保证一致性的情况下判断哪些entry已废弃并删除，最终导致内存泄露

	// 为什么notifyChanMap不使用index作为key？
	// 在网络分区时，可能因为log被覆盖导致同一index的log不相同，后覆盖的log apply成功导致被覆盖的指令对应的channel被通知
	// 可能引发错误的RPC reply
	notifyChanMap map[int64]chan Notification

	lastAppliedIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Operation: OpGet,
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		CommandId: args.CommandId,
	}

	kv.mu.Lock()
	_, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	DPrintf(dServer, "S%d receive valid Get RPC from C%d, R%d", kv.me, args.ClientId, args.RequestId)

	notifyChan := make(chan Notification, 1)
	kv.notifyChanMap[op.CommandId] = notifyChan
	kv.mu.Unlock()

	// when leader changed (term changed), it should redirect immediately
	go kv.termDetector(term, op.CommandId)

	select {
	case notification := <-notifyChan:
		reply.Err = notification.err
		reply.Value = notification.value
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrWrongLeader // 超时，可能是leader变更了
	}

	kv.mu.Lock()
	delete(kv.notifyChanMap, op.CommandId)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		CommandId: args.CommandId,
	}
	if args.Op == "Put" {
		op.Operation = OpPut
	} else {
		op.Operation = OpAppend
	}

	kv.mu.Lock()
	_, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	DPrintf(dServer, "S%d receive valid %s RPC from C%d, R%d", kv.me, args.Op, args.ClientId, args.RequestId)

	notifyChan := make(chan Notification, 1)
	kv.notifyChanMap[op.CommandId] = notifyChan
	kv.mu.Unlock()

	// when leader changed (term changed), it should redirect immediately
	go kv.termDetector(term, op.CommandId)

	select {
	case notification := <-notifyChan:
		reply.Err = notification.err
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrWrongLeader // 超时，可能是leader变更了
	}

	kv.mu.Lock()
	delete(kv.notifyChanMap, op.CommandId)
	kv.mu.Unlock()
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if !msg.CommandValid {
			if msg.SnapshotValid {
				kv.installSnapshot(msg)
			}
			continue
		}

		notification := Notification{}
		notification.err = OK
		op := msg.Command.(Op)

		kv.mu.Lock()
		lastRequestId, exist := kv.dupMap[op.ClientId]
		duplicate := exist && lastRequestId >= op.RequestId
		if !duplicate {
			switch op.Operation {
			case OpGet:
				if value, ok := kv.store[op.Key]; ok {
					notification.value = value
				} else {
					notification.err = ErrNoKey
				}
				DPrintf(dServer, "S%d done Get operation, log I%d, R%d, current key: %s, current value: %s",
					kv.me, msg.CommandIndex, op.RequestId, op.Key, kv.store[op.Key])
			case OpPut:
				kv.store[op.Key] = op.Value
				DPrintf(dServer, "S%d done Put operation, log I%d, R%d, current key: %s, current value: %s",
					kv.me, msg.CommandIndex, op.RequestId, op.Key, kv.store[op.Key])
			case OpAppend:
				kv.store[op.Key] = kv.store[op.Key] + op.Value
				DPrintf(dServer, "S%d done Append operation, log I%d, R%d, current key: %s, current value: %s",
					kv.me, msg.CommandIndex, op.RequestId, op.Key, kv.store[op.Key])
			}
			kv.dupMap[op.ClientId] = op.RequestId
			kv.lastAppliedIndex = msg.CommandIndex
			kv.snapshotTrigger()
		} else if op.Operation == OpGet {
			DPrintf(dServer, "S%d find duplicate log I%d, R%d <= %d, operation: %d", kv.me, msg.CommandIndex,
				op.RequestId, lastRequestId, op.Operation)
			if value, ok := kv.store[op.Key]; ok {
				notification.value = value
			} else {
				notification.err = ErrNoKey
			}
		} else {
			DPrintf(dServer, "S%d find duplicate log I%d, R%d <= %d, operation: %d", kv.me, msg.CommandIndex,
				op.RequestId, lastRequestId, op.Operation)
		}
		notifyChan, ok := kv.notifyChanMap[op.CommandId]
		if ok {
			// 必须使用非阻塞发送，因为RPC处理器可能已经超时
			select {
			case notifyChan <- notification:
			default:
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) installSnapshot(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ok := kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot)
	if ok {
		s := bytes.NewBuffer(msg.Snapshot)
		sd := labgob.NewDecoder(s)
		var snapshot Snapshot
		sd.Decode(&snapshot)
		kv.store = snapshot.Store
		kv.lastAppliedIndex = snapshot.Index
		kv.dupMap = snapshot.DupMap
		DPrintf(dServer, "S%d installed snapshot, snapshot to I%d", kv.me, kv.lastAppliedIndex)
	}
}

func (kv *KVServer) termDetector(copyTerm int, commandId int64) {
	for !kv.killed() {
		kv.mu.Lock()
		notifyChan, exist := kv.notifyChanMap[commandId]
		if !exist {
			kv.mu.Unlock()
			return
		}
		if currentTerm, _ := kv.rf.GetState(); currentTerm != copyTerm {
			DPrintf(dServer, "Detect term changed, current T%d != T%d", currentTerm, copyTerm)
			notification := Notification{}
			notification.err = ErrWrongLeader
			select {
			case notifyChan <- notification:
			default:
			}
		}
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// must be called within lock area
func (kv *KVServer) snapshotTrigger() {
	if kv.killed() || kv.maxraftstate < 0 {
		return
	}

	if kv.persister.RaftStateSize() <= kv.maxraftstate {
		return
	}

	DPrintf(dServer, "S%d Reach snapshot threshold, snapshot to I%d", kv.me, kv.lastAppliedIndex)

	snapshot := Snapshot{
		Store:  kv.store,
		Index:  kv.lastAppliedIndex,
		DupMap: kv.dupMap,
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(snapshot)
	snapshotBytes := w.Bytes()

	kv.rf.Snapshot(kv.lastAppliedIndex, snapshotBytes)
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.store = make(map[string]string)
	kv.dupMap = make(map[int64]int64)
	kv.notifyChanMap = make(map[int64]chan Notification)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.SetDebugType("kvraft")

	kv.lastAppliedIndex = 0
	if snapshotBytes := persister.ReadSnapshot(); len(snapshotBytes) != 0 {
		s := bytes.NewBuffer(snapshotBytes)
		sd := labgob.NewDecoder(s)
		var snapshot Snapshot
		sd.Decode(&snapshot)
		kv.store = snapshot.Store
		kv.lastAppliedIndex = snapshot.Index
		kv.dupMap = snapshot.DupMap
	}

	go kv.applier()
	DPrintf(dServer, "KV Server S%d initiated", me)
	return kv
}
