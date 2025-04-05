package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"raft-shard-kv/src/labgob"
	"raft-shard-kv/src/labrpc"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// currentRole的枚举定义
const (
	follower int = iota
	candidate
	leader
)

// 超时期限（ms）
const (
	timeoutMin = 600
	timeoutMax = 800
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	dead      int32

	// Leader election
	CurrentTerm       int       // 当前（已知最新的）任期
	Voted             bool      // 当前任期是否已投过通过票
	VotedFor          int       // 当前任期投给的candidate的ID
	currentRole       int       // 当前角色（0-follower；1-candidate；2-leader）
	lastHeartbeatTime time.Time // 上次从leader接收到心跳的时间

	// Log replication
	Log         []LogEntry    // 日志（index从1开始）
	commitIndex int           // 已commit的日志条目的最高index（单调递增）
	lastApplied int           // 已apply至状态机的日志条目的最高index（单调递增）
	nextIndex   []int         // 每个节点的下一个日志复制的位置index（仅leader 易失）
	matchIndex  []int         // 每个节点的最高已复制日志的index（仅leader 易失 单调递增）
	applyCh     chan ApplyMsg // 与client通信的管道
	cond        *sync.Cond    // 用于控制当前是否该开启log replicate的条件变量
	logSending  bool          // 指示当前是否有log replicate正在执行

	// Snapshot
	snapshot          []byte // 当前的快照
	lastIncludedIndex int    // 快照replace的last index
	lastIncludedTerm  int    // 对应的term

	// logger
	debugType string
}

type LogEntry struct {
	Command interface{}
	Term    int
}

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.CurrentTerm
	isleader = rf.currentRole == leader
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.Voted)
	e.Encode(rf.VotedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	DPrintf(rf.debugType, dPersist, "S%d Persist state finished", rf.me)
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.Voted)
	e.Encode(rf.VotedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
	DPrintf(rf.debugType, dPersist, "S%d Persist state and snapshot finished", rf.me)
}

func (rf *Raft) getActualIndex(literalIndex int) int {
	// 存在actualIndex < 0的情况，此时literalIndex对应的entry已经被snapshot
	if rf.snapshot != nil {
		return literalIndex - rf.lastIncludedIndex
	}
	return literalIndex
}

func (rf *Raft) getLiteralIndex(actualIndex int) int {
	if rf.snapshot != nil {
		return rf.lastIncludedIndex + actualIndex
	}
	return actualIndex
}

// restore previously persisted state.
func (rf *Raft) readPersist() {
	data := rf.persister.ReadRaftState()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voted bool
	var votedFor int
	var lastIncludedIndex int
	var lastIncludedTerm int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&voted) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil || d.Decode(&log) != nil {
		DPrintf(rf.debugType, dPersist, "Read persist error")
	} else {
		rf.snapshot = rf.persister.ReadSnapshot()
		rf.CurrentTerm = currentTerm
		rf.Voted = voted
		rf.VotedFor = votedFor
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.Log = log
		if len(rf.snapshot) != 0 {
			rf.commitIndex = lastIncludedIndex
			rf.lastApplied = lastIncludedIndex
		}
		DPrintf(rf.debugType, dPersist, "S%d Read persist finished, current term T%d", rf.me, rf.CurrentTerm)
	}
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	if lastIncludedIndex < rf.lastIncludedIndex || (lastIncludedIndex >= rf.lastIncludedIndex && lastIncludedTerm < rf.lastIncludedTerm) {
		DPrintf(rf.debugType, dSnapshot, "S%d rejected InstSnap, server: I%d T%d, self: I%d T%d", rf.me, lastIncludedIndex,
			lastIncludedTerm, rf.lastIncludedIndex, rf.lastIncludedTerm)
		rf.mu.Unlock()
		return false
	}
	if lastIncludedIndex < rf.getLiteralIndex(len(rf.Log)-1) {
		DPrintf(rf.debugType, dSnapshot, "S%d rejected InstSnap, server: I%d, self: I%d", rf.me, lastIncludedIndex,
			rf.getLiteralIndex(len(rf.Log)-1))
		rf.mu.Unlock()
		return false
	}
	rf.Log = make([]LogEntry, 1)
	rf.snapshot = snapshot
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastHeartbeatTime = time.Now()
	DPrintf(rf.debugType, dSnapshot, "S%d accepted InstSnap, new snapshot: I%d T%d", rf.me, lastIncludedIndex, lastIncludedTerm)

	// 持久化状态和快照
	rf.persistStateAndSnapshot(snapshot)

	rf.mu.Unlock()
	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果index小于等于lastIncludedIndex，说明这个snapshot已经过时了
	if index <= rf.lastIncludedIndex {
		return
	}
	actualIndex := rf.getActualIndex(index)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.Log[actualIndex].Term
	newLog := make([]LogEntry, 1)
	rf.Log = append(newLog, rf.Log[actualIndex+1:]...)
	rf.snapshot = snapshot

	rf.persistStateAndSnapshot(snapshot)
	DPrintf(rf.debugType, dSnapshot, "S%d done snapshot to I%d", rf.me, index)

}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.CurrentTerm {
		DPrintf(rf.debugType, dTerm, "S%d Find bigger term (%d>%d), converting to follower", rf.me, args.Term, rf.CurrentTerm)
		rf.CurrentTerm = args.Term
		rf.currentRole = follower
		rf.Voted = false
		rf.persist()
	}

	// 不应该在此处重置心跳超时，错误的snapshot也可能导致timeout重置
	//rf.lastHeartbeatTime = time.Now()
	reply.Term = rf.CurrentTerm

	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      clone(args.Snapshot),
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.mu.Unlock()
	rf.applyCh <- msg
	DPrintf(rf.debugType, dSnapshot, "S%d received InstSnap", rf.me)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf(rf.debugType, dSnapshot, "S%d => S%d Sending InstSnap LII:%d LIT:%d", args.LeaderId, server,
		args.LastIncludedIndex, args.LastIncludedTerm)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.CurrentTerm {
		DPrintf(rf.debugType, dTerm, "S%d Find bigger term (%d>%d), converting to follower", rf.me, reply.Term, rf.CurrentTerm)
		rf.CurrentTerm = reply.Term
		rf.currentRole = follower
		rf.lastHeartbeatTime = time.Now()
		rf.Voted = false
		rf.persist()
		return
	}

	// 只有当仍然是 leader 时才更新 matchIndex 和 nextIndex
	if rf.currentRole == leader {
		if rf.matchIndex[server] < args.LastIncludedIndex {
			rf.matchIndex[server] = args.LastIncludedIndex
		}
		if rf.nextIndex[server] <= args.LastIncludedIndex {
			rf.nextIndex[server] = args.LastIncludedIndex + 1
		}
	}
}

type RequestVoteArgs struct {
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	// 处理请求投票逻辑
	// 若接收到的term比当前的大
	if args.Term > rf.CurrentTerm {
		DPrintf(rf.debugType, dTerm, "S%d Find bigger term (%d>%d), converting to follower", rf.me, args.Term, rf.CurrentTerm)
		// 设置term并转换为follower
		rf.CurrentTerm = args.Term
		rf.currentRole = follower
		rf.Voted = false
	}
	// 坑: term比自己大但投了拒绝票，不应该重置election timeout，否则可能会导致日志陈旧的节点阻止日志较新节点开启选举
	// 若恰好日志陈旧的节点的超时时间比其他节点超时时间都短，则会陷入死循环
	// 若接收到的term比当前的小（不是小于等于）
	if args.Term < rf.CurrentTerm {
		// 投拒绝票
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		DPrintf(rf.debugType, dVote, "S%d Deny voting to S%d at T%d", rf.me, args.CandidateId, rf.CurrentTerm)
	} else {
		// 等于的情况（可能为转换而来的相等，或是原本就相等）
		// 对于term相等的情况的讨论：若当前为follower，不影响；
		// 对于candidate和leader，必然已经投给自己，会拒绝投票

		// 若该任期未投票，或已投过给该人（考虑网络请求重复），则下一步判断，否则投拒绝票
		if !rf.Voted || rf.VotedFor == args.CandidateId {
			newestTerm := rf.Log[len(rf.Log)-1].Term
			if len(rf.Log)-1 == 0 && rf.snapshot != nil {
				newestTerm = rf.lastIncludedTerm
			}
			if args.LastLogTerm > newestTerm {
				// 投通过票
				DPrintf(rf.debugType, dVote, "S%d Granting vote to S%d at T%d", rf.me, args.CandidateId, rf.CurrentTerm)
				reply.Term = rf.CurrentTerm
				reply.VoteGranted = true
				rf.Voted = true
				rf.VotedFor = args.CandidateId
				// 重置timeout counter
				rf.lastHeartbeatTime = time.Now()
			} else if args.LastLogTerm == newestTerm {
				if args.LastLogIndex >= rf.getLiteralIndex(len(rf.Log)-1) {
					// 投通过票
					DPrintf(rf.debugType, dVote, "S%d Granting vote to S%d at T%d", rf.me, args.CandidateId, rf.CurrentTerm)
					reply.Term = rf.CurrentTerm
					reply.VoteGranted = true
					rf.Voted = true
					rf.VotedFor = args.CandidateId
					// 重置timeout counter
					rf.lastHeartbeatTime = time.Now()
				} else {
					// 投拒绝票
					reply.Term = rf.CurrentTerm
					reply.VoteGranted = false
					DPrintf(rf.debugType, dVote, "S%d Deny voting to S%d at T%d", rf.me, args.CandidateId, rf.CurrentTerm)
				}
			} else {
				// 投拒绝票
				reply.Term = rf.CurrentTerm
				reply.VoteGranted = false
				DPrintf(rf.debugType, dVote, "S%d Deny voting to S%d at T%d", rf.me, args.CandidateId, rf.CurrentTerm)
			}
		} else {
			// 投拒绝票
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = false
			DPrintf(rf.debugType, dVote, "S%d Deny voting to S%d at T%d", rf.me, args.CandidateId, rf.CurrentTerm)
		}
	}
	rf.persist()
	rf.mu.Unlock()
}

// 发送请求投票RPC并处理回复（Candidate）
func (rf *Raft) sendRequestVote(server int, c chan<- int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		// 处理接收投票逻辑
		// 若接收到的term比当前的大
		if reply.Term > rf.CurrentTerm {
			DPrintf(rf.debugType, dTerm, "S%d Find bigger term (%d>%d), converting to follower", rf.me, reply.Term, rf.CurrentTerm)
			// 设置term并转换为follower
			rf.CurrentTerm = reply.Term
			rf.currentRole = follower
			rf.lastHeartbeatTime = time.Now()
			rf.Voted = false
			rf.persist()
		}
		rf.mu.Unlock()
		// 若接收到有效投票
		if reply.VoteGranted {
			DPrintf(rf.debugType, dTerm, "S%d <- S%d Got vote for T%d", rf.me, server, args.Term)
			// 向counterChan中发送信号
			c <- 1
		}
	}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// 处理追加条目逻辑

	// 若接收到的term比当前的大（对于candidate，则是大于等于）
	if args.Term > rf.CurrentTerm {
		// 设置term并转换为follower
		DPrintf(rf.debugType, dTerm, "S%d Find bigger term (%d>%d), converting to follower", rf.me, args.Term, rf.CurrentTerm)
		rf.CurrentTerm = args.Term
		rf.currentRole = follower
		rf.Voted = false
		rf.persist()
	} else if rf.currentRole == candidate && args.Term == rf.CurrentTerm {
		// 转换为follower
		DPrintf(rf.debugType, dVote, "S%d Find existing leader, converting to follower", rf.me)
		rf.currentRole = follower
	}

	// 注意：收到来自term比自己小的leader的心跳，不要重置election timer，防止错误的leader持续工作
	if args.Term < rf.CurrentTerm || rf.getActualIndex(args.PrevLogIndex) < 0 {
		reply.Success = false
		DPrintf(rf.debugType, dTimer, "S%d receive invalid AppEnt from S%d, T%d", rf.me, args.LeaderId, args.Term)
	} else if args.PrevLogIndex > rf.getLiteralIndex(len(rf.Log)-1) ||
		(rf.getActualIndex(args.PrevLogIndex) > 0 && rf.Log[rf.getActualIndex(args.PrevLogIndex)].Term != args.PrevLogTerm) ||
		(rf.getActualIndex(args.PrevLogIndex) == 0 && rf.lastIncludedTerm != args.PrevLogTerm) {
		// index大于当前日志长度或条目不匹配，则进入该分支，返回false，等待leader重新发送
		// log为空时：index和term均为0，此时必定成功，不会进入该分支
		reply.Success = false
		index := rf.getActualIndex(args.PrevLogIndex)
		if index > len(rf.Log)-1 {
			index = len(rf.Log) - 1
		}
		// find first conflict index
		for i := index; i >= 0; i-- {
			if rf.Log[i].Term != rf.Log[index].Term {
				reply.ConflictIndex = rf.getLiteralIndex(i + 1)
				break
			}
		}
		// 若没有找到冲突点，则设置为1
		if reply.ConflictIndex == 0 {
			reply.ConflictIndex = 1
		}
		DPrintf(rf.debugType, dTimer, "S%d receive mismatched AppEnt from S%d, T%d, conflictIndex: %d", rf.me, args.LeaderId, args.Term, reply.ConflictIndex)
		rf.lastHeartbeatTime = time.Now()
	} else {
		reply.Success = true
		DPrintf(rf.debugType, dTimer, "S%d receive valid AppEnt from S%d, T%d", rf.me, args.LeaderId, args.Term)
		rf.lastHeartbeatTime = time.Now()
		// 无论是否有entries（是否是心跳），都进入该循环，判断并复制日志
		for i := range args.Entries {
			// 依次判断是否重复，若重复则删除后面的所有log，并开始append
			// 判断是否长度超过rf.log（即后面没有其它entries）
			if rf.getActualIndex(args.PrevLogIndex+1+i) > len(rf.Log)-1 {
				// 若log后面无其它entries，直接append剩余的entries
				// 此处使用深拷贝，而不是使用切片引用，如rf.Log=append(rf.Log,args.Entries[i:]...)
				// 若使用切片引用，则rf.Log和args.Entries[i:]会指向同一个底层数组，导致竞态
				entriesToAppend := make([]LogEntry, len(args.Entries[i:]))
				copy(entriesToAppend, args.Entries[i:])
				rf.Log = append(rf.Log, entriesToAppend...)
				break
			}
			// 若日志有冲突
			if rf.Log[rf.getActualIndex(args.PrevLogIndex+1+i)].Term != args.Entries[i].Term {
				// 删除后面所有的log，并直接append剩余entries
				rf.Log = rf.Log[:rf.getActualIndex(args.PrevLogIndex+1+i)]
				entriesToAppend := make([]LogEntry, len(args.Entries[i:]))
				copy(entriesToAppend, args.Entries[i:])
				rf.Log = append(rf.Log, entriesToAppend...)
				break
			}
			// 否则往后遍历，直到找到冲突点或空槽
		}
		if len(args.Entries) != 0 {
			DPrintf(rf.debugType, dLog2, "S%d new log: %s", rf.me, EntriesToString(rf.Log))
			rf.persist()
		}
	}

	// 若未失败，根据leader的commitIndex来更新自己的commitIndex
	if reply.Success && args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.getLiteralIndex(len(rf.Log)-1) {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.getLiteralIndex(len(rf.Log) - 1)
		}
		DPrintf(rf.debugType, dLog2, "S%d change commitIndex to I%d", rf.me, rf.commitIndex)
	}
	reply.Term = rf.CurrentTerm
	rf.mu.Unlock()
}

// 发送AppendEntries RPC并处理回复（Leader）
// 返回值：ok, success
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (bool, bool) {
	DPrintf(rf.debugType, dLog1, "S%d -> S%d Sending AppEnt PLI:%d PLT:%d LC:%d - [%s]",
		rf.me, server, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, EntriesToString(args.Entries))
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	success := false
	if ok {
		rf.mu.Lock()
		// 处理接收投票逻辑
		// 若接收到的term比当前的大
		if reply.Term > rf.CurrentTerm {
			// 设置term并转换为follower
			DPrintf(rf.debugType, dTerm, "S%d Find bigger term (%d>%d), converting to follower", rf.me, reply.Term, rf.CurrentTerm)
			rf.CurrentTerm = reply.Term
			rf.currentRole = follower
			rf.lastHeartbeatTime = time.Now()
			rf.Voted = false
			rf.persist()
		}

		if reply.Success {
			success = true
		}
		rf.mu.Unlock()
	}
	return ok, success
}

func EntriesToString(entries []LogEntry) string {
	var parts []string
	for _, entry := range entries {
		parts = append(parts, fmt.Sprintf("(%v, T%d)", entry.Command, entry.Term))
	}
	return strings.Join(parts, ",")
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	// 下一个放置log entry的位置（index从1开始）
	index := rf.getLiteralIndex(len(rf.Log))
	term := rf.CurrentTerm
	isLeader := rf.currentRole == leader

	if isLeader {
		DPrintf(rf.debugType, dLog1, "S%d receive new command (%v) at, I%d", rf.me, command, index)
		entry := LogEntry{command, term}
		rf.Log = append(rf.Log, entry)
		DPrintf(rf.debugType, dLog2, "S%d new log: %s", rf.me, EntriesToString(rf.Log))
		rf.persist()
		rf.cond.Broadcast()
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

// 用于发送日志给follower
// 合并部分log，不会再每收到一次command就调用一次完整的日志复制操作
// 防止收到并发的command时，有冗余的sendLogEntries运行的情况
func (rf *Raft) logSender() {
	for {
		rf.mu.Lock()
		// 开始复制日志的三个条件：当前是leader，有新条目可以发送，没有正在发送的条目
		// 任一条件不满足则会等待
		// 坑：logSending不会转为false：counterChan接收的投票因部分node killed所以始终达不到半数
		for rf.currentRole != leader || rf.getLiteralIndex(len(rf.Log)-1) <= rf.commitIndex || rf.logSending {
			rf.cond.Wait()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
		}

		rf.logSending = true
		// 为logSending设置timeout，若超时且当前复制过程仍未结束，则置为false
		// 此处是为了防止网络请求的时间长，不可避免会有长时间等待，导致logSending缓慢改变->下一次日志复制等待很久
		go func(copyCommitIndex int) {
			time.Sleep(100 * time.Millisecond)
			rf.mu.Lock()
			// 此时commit可能已经前进（在commit过半时已经置为false，开始了下一轮复制），则不再置false
			if copyCommitIndex == rf.commitIndex {
				rf.logSending = false
				rf.cond.Broadcast()
			}
			rf.mu.Unlock()
		}(rf.commitIndex)

		// 要发送的log entry的index
		index := rf.getLiteralIndex(len(rf.Log) - 1)
		copyTerm := rf.CurrentTerm

		DPrintf(rf.debugType, dLog1, "S%d start replicating entry at I%d", rf.me, index)

		// 创建用于goroutine间通信的channel；创建WaitGroup，以使所有操作完成后关闭channel
		counterChan := make(chan int, len(rf.peers))
		var wg sync.WaitGroup
		wg.Add(len(rf.peers) - 1) // 添加需要等待的goroutine数（除了自己外的peers数）

		// 创建goroutine判断该entry是否复制过半，处理commit相关的操作
		go rf.checkCommit(len(rf.peers), counterChan, index)

		// 向各个节点发送日志条目
		for i := range rf.peers {
			if i != rf.me {
				go rf.sendLogEntries(i, copyTerm, index, &wg, counterChan)
			}
		}

		// 创建收尾goroutine，等待所有日志复制尝试结束后，关闭channel
		go func() {
			wg.Wait()
			close(counterChan)
		}()
		rf.mu.Unlock()
	}
}

// 根据entries复制的结果推进commitIndex
func (rf *Raft) checkCommit(nodeNum int, c <-chan int, index int) {
	counter := 1
	// 读取counterChan中的内容直到关闭/跳出
	for i := range c {
		counter += i
		// 两整数相除，此处自动向下取整
		// 超过半数则日志可commit
		if counter > nodeNum/2 {
			rf.mu.Lock()
			// 坑：此处commit时需要检查index是否比自身的commitIndex大
			// 存在情况：并发执行多个start，后来的（commitIndex较大的）任务先完成，导致后续完成的小index覆盖原有的大index
			if index > rf.commitIndex {
				rf.commitIndex = index
			}
			DPrintf(rf.debugType, dLog2, "S%d change commitIndex to I%d", rf.me, rf.commitIndex)
			rf.logSending = false
			rf.cond.Broadcast()
			rf.mu.Unlock()
			// 已过半，停止循环读取channel，结束该goroutine
			break
		}
	}
}

// 不断循环，判断是否有新的日志条目可以apply至状态机（发送至applyCh）
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			nextApplyIndex := rf.lastApplied + 1
			actualIndex := rf.getActualIndex(nextApplyIndex)
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[actualIndex].Command,
				CommandIndex: nextApplyIndex,
			}
			rf.lastApplied = nextApplyIndex
			DPrintf(rf.debugType, dLog2, "S%d applied (%v,I%d)", rf.me, msg.Command, msg.CommandIndex)
			// 快速重试，防止有entries等待apply
			rf.mu.Unlock()
			// 此处需要在锁外操作，否则触发snapshot时，snapshot()中获取rf.mu，死锁
			rf.applyCh <- msg
			continue
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) sendLogEntries(server int, copyTerm int, index int, wg *sync.WaitGroup, c chan<- int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.currentRole != leader || rf.CurrentTerm != copyTerm {
			rf.mu.Unlock()
			break
		}
		// 若有需要发送的日志条目
		if index >= rf.nextIndex[server] {
			// 初始化args和reply
			// 此时的index为已经append新条目之后的index
			acceptIndex := rf.nextIndex[server]
			args := AppendEntriesArgs{}
			args.LeaderId = rf.me
			args.Term = copyTerm
			args.PrevLogIndex = acceptIndex - 1

			actualIndex := rf.getActualIndex(args.PrevLogIndex)
			if actualIndex == 0 && rf.snapshot != nil {
				args.PrevLogTerm = rf.lastIncludedTerm
				entries := rf.Log[1:]
				args.Entries = make([]LogEntry, len(entries))
				copy(args.Entries, entries)
			} else if actualIndex < 0 {
				// 触发install snapshot
				ssArgs := InstallSnapshotArgs{
					Term:              copyTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Snapshot:          rf.snapshot,
				}
				ssReply := InstallSnapshotReply{}
				rf.mu.Unlock()
				rf.sendInstallSnapshot(server, &ssArgs, &ssReply)
				break
				// warning: 存在一种罕见的可能：
				// S0 leader，S0 -> S1 snapshot, S0 -> S2 send log
				// 其中S1的branch很快触发wg.done()，S2发送完后也触发c <- 1与wg.done()，但因goroutine调度问题，
				// 收尾goroutine先于checkCommit()运行，导致channel在被读取前关闭，使得日志成功复制但无法触发leader的commit
				// 但后续进入的命令会覆盖这一短暂的错误
			} else {
				args.PrevLogTerm = rf.Log[actualIndex].Term
				entries := rf.Log[actualIndex+1:]
				args.Entries = make([]LogEntry, len(entries))
				copy(args.Entries, entries)
			}

			args.LeaderCommit = rf.commitIndex
			reply := AppendEntriesReply{}
			rf.mu.Unlock()
			ok, success := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				break
			} else {
				if success {
					c <- 1
					rf.mu.Lock()
					if index > rf.matchIndex[server] {
						rf.matchIndex[server] = index
						rf.nextIndex[server] = index + 1
					}
					rf.mu.Unlock()
					break
				} else {
					rf.mu.Lock()
					rf.nextIndex[server] = reply.ConflictIndex
					// 当leader invalid时，conflictIndex为0（未设置的默认值）
					if rf.nextIndex[server] == 0 {
						rf.nextIndex[server] = 1
					}

					rf.mu.Unlock()
					continue
				}
			}
		}
		rf.mu.Unlock()
	}
	wg.Done()
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received heartbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// 在范围内随机取一个timeout时间
		timeout := timeoutMin + rand.Int63n(timeoutMax-timeoutMin)
		// 循环直到超时
		for {
			// 检查当前是否被kill
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			// 超时且当前不为leader
			if time.Since(rf.lastHeartbeatTime).Milliseconds() > timeout && rf.currentRole != leader {
				// 此处本应有的unlock后移至函数末尾，以便在同一个lock中完成所有操作
				break
			}
			rf.mu.Unlock()
			time.Sleep(30 * time.Millisecond)
		}
		// 自增任期
		rf.CurrentTerm++
		// 给自己投票
		rf.currentRole = candidate
		rf.Voted = true
		rf.VotedFor = rf.me
		// 重置计时器
		rf.lastHeartbeatTime = time.Now()

		DPrintf(rf.debugType, dTimer, "S%d Election timeout, become candidate for T%d", rf.me, rf.CurrentTerm)

		// 保存currentTerm的副本
		tmpTerm := rf.CurrentTerm

		// 创建用于goroutine间通信的channel；创建WaitGroup，以使所有操作完成后关闭channel
		counterChan := make(chan int, len(rf.peers))
		var wg sync.WaitGroup
		wg.Add(len(rf.peers) - 1) // 添加需要等待的goroutine数（除了自己外的peers数）

		// 创建goroutine判断票数是否过半，并处理转换为leader的相关操作
		go rf.checkMajority(len(rf.peers), counterChan, tmpTerm)

		// 向所有其他节点发送请求投票RPC
		DPrintf(rf.debugType, dVote, "S%d Requesting vote", rf.me)
		for i := range rf.peers {
			if i != rf.me {
				go func(copyTerm int, c chan<- int, index int) {
					rf.mu.Lock()
					// 初始化args和reply
					args := RequestVoteArgs{}
					args.CandidateId = rf.me
					args.Term = copyTerm // 不能简单地使用rf.CurrentTerm，因为可能已经发生改变
					if len(rf.Log)-1 == 0 && rf.snapshot != nil {
						args.LastLogIndex = rf.lastIncludedIndex
						args.LastLogTerm = rf.lastIncludedTerm
					} else {
						args.LastLogIndex = rf.getLiteralIndex(len(rf.Log) - 1)
						args.LastLogTerm = rf.Log[len(rf.Log)-1].Term
					}
					reply := RequestVoteReply{}
					rf.mu.Unlock()
					rf.sendRequestVote(index, c, &args, &reply) // 此处不在加锁区内，无需担心call操作耗时

					// 发送并处理完投票后，令waitGroup-1
					wg.Done()
				}(tmpTerm, counterChan, i)
			}
		}
		rf.persist()
		rf.mu.Unlock()

		// 创建收尾goroutine，等待所有投票处理完后，关闭channel
		// 坑：若不创建goroutine，candidate在等待选举的情况下，其timeout时间会远超预期（等待waitgroup）
		go func() {
			wg.Wait()
			close(counterChan)
		}()
	}
}

// 判断票数是否过半，并处理转换为leader的相关操作
func (rf *Raft) checkMajority(nodeNum int, c <-chan int, copyTerm int) {
	counter := 1
	// 读取counterChan中的内容直到关闭/跳出
	for i := range c {
		counter += i
		// 两整数相除，此处自动向下取整
		// 超过半数且任期未过期，则成为leader，并结束循环
		if counter > nodeNum/2 {
			rf.mu.Lock()
			// 若选举结果出来后还是当前任期
			if copyTerm == rf.CurrentTerm {
				// 转换为leader，且无需修改voted（任期没变）
				DPrintf(rf.debugType, dLeader, "S%d Achieved majority for T%d (%d), converting to leader", rf.me, rf.CurrentTerm, counter)
				rf.currentRole = leader
				// 初始化nextIndex和matchIndex
				rf.nextIndex = make([]int, len(rf.peers))
				for i := range rf.nextIndex {
					rf.nextIndex[i] = rf.getLiteralIndex(len(rf.Log))
				}
				rf.matchIndex = make([]int, len(rf.peers))
				rf.logSending = false
				// 向所有其他节点定期发送空的AppendEntries RPC（心跳）
				// leader身份快速转变时（leader -> ? -> leader）会存在两个相同的heartbeat goroutine吗？会，且旧的goroutine
				// 错误F：leader能且只能因收到更大的term转换为follower，不可能在heartbeat timeout间完成候选与选举
				// 正确T：leader转为follower，但未给candidate投同意票，随后马上超时，成为下一任leader
				for j := range rf.peers {
					if j != rf.me {
						go rf.heartbeatTicker(j, copyTerm)
					}
				}
			}
			rf.persist()
			rf.mu.Unlock()
			// 票数已过半，无论是否有效，都应停止循环读取channel，结束该goroutine
			break
		}
	}
}

// 向指定节点定期发送心跳
func (rf *Raft) heartbeatTicker(server int, copyTerm int) {
	// 坑：某个server被kill之后，ticker goroutine仍在运行，出现无限发送心跳等情况
	// 故还需检查是否已被kill
	for !rf.killed() {
		// 若当前不再是leader，跳出循环
		// 坑：leader快速变换身份，又迅速转变回leader，导致重复的heartbeatTicker运行
		// 故还需检查任期是否为创建时的任期
		rf.mu.Lock()
		if rf.currentRole != leader || rf.CurrentTerm != copyTerm {
			rf.mu.Unlock()
			break
		}

		// 若当前没有日志在发送（防止冗余的一致性同步）
		if !rf.logSending {
			// 发送心跳，并完成一致性检查
			go func() {
				for !rf.killed() {
					rf.mu.Lock()
					if rf.currentRole != leader {
						rf.mu.Unlock()
						return
					}
					index := rf.getLiteralIndex(len(rf.Log) - 1) // 最后一个entry的index
					// 初始化args和reply
					args := AppendEntriesArgs{}
					args.LeaderId = rf.me
					args.Term = copyTerm // 不能简单地使用rf.CurrentTerm，因为可能已经发生改变
					args.LeaderCommit = rf.commitIndex
					args.PrevLogIndex = rf.nextIndex[server] - 1
					actualIndex := rf.getActualIndex(args.PrevLogIndex)
					if actualIndex == 0 && rf.snapshot != nil {
						args.PrevLogTerm = rf.lastIncludedTerm
						// when normal 为空切片（心跳）
						entries := rf.Log[1:]
						args.Entries = make([]LogEntry, len(entries))
						copy(args.Entries, entries)
					} else if actualIndex < 0 {
						// 触发install snapshot
						ssArgs := InstallSnapshotArgs{
							Term:              copyTerm,
							LeaderId:          rf.me,
							LastIncludedIndex: rf.lastIncludedIndex,
							LastIncludedTerm:  rf.lastIncludedTerm,
							Snapshot:          rf.snapshot,
						}
						ssReply := InstallSnapshotReply{}
						rf.mu.Unlock()
						rf.sendInstallSnapshot(server, &ssArgs, &ssReply)
						break
					} else {
						args.PrevLogTerm = rf.Log[actualIndex].Term
						// when normal 为空切片（心跳）
						entries := rf.Log[actualIndex+1:]
						args.Entries = make([]LogEntry, len(entries))
						copy(args.Entries, entries)
					}
					reply := AppendEntriesReply{}
					rf.mu.Unlock()
					ok, success := rf.sendAppendEntries(server, &args, &reply)
					if !ok {
						break
					} else {
						if success {
							rf.mu.Lock()
							if rf.matchIndex[server] < index {
								rf.matchIndex[server] = index
								rf.nextIndex[server] = index + 1
							}
							rf.mu.Unlock()
							break
						} else {
							rf.mu.Lock()
							rf.nextIndex[server] = reply.ConflictIndex
							// 当leader invalid时，conflictIndex为0（未设置的默认值）
							if rf.nextIndex[server] == 0 {
								rf.nextIndex[server] = 1
							}
							rf.mu.Unlock()
							continue
						}
					}
				}
			}()
		}

		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) SetDebugType(debugType string) {
	rf.debugType = debugType
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.SetDebugType("raft")

	rf.CurrentTerm = 0
	rf.Voted = false
	rf.currentRole = follower
	rf.lastHeartbeatTime = time.Now()

	// 对log先填充一个占位元素，这样其初始长度为1，index从1开始
	rf.Log = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	// 坑：注意初始化applyCh
	rf.applyCh = applyCh
	rf.cond = sync.NewCond(&rf.mu)

	rf.snapshot = nil
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist()

	// 启动log复制goroutine
	go rf.logSender()

	go rf.applier()

	// start ticker goroutine to start elections
	go rf.ticker()

	DPrintf(rf.debugType, dBootstrap, "Raft Server S%d initiated", me)

	return rf
}
