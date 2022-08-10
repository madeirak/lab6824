package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"mit-6.824/labgob"
	"mit-6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool // true为log，false为snapshot

	// 向application层提交日志
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// 向application层安装快照
	Snapshot          []byte
	LastIncludedIndex int
	LastIncludedTerm  int
}

// 日志项
type LogEntry struct {
	Command interface{}
	Term    int
}

// 当前角色
const ROLE_LEADER = "Leader"
const ROLE_FOLLOWER = "Follower"
const ROLE_CANDIDATES = "Candidates"

const (
	/**
	  论文第 5.2 节提到选举超时应该在 150 到 300 毫秒范围内。
	  只有当 Leader 发送一次心跳包的远小于 150 毫秒，这种范围才有意义。
	  由于测试将您发送心跳包的频率限制在 10 次/秒内（译者注：也就是大于 100 毫秒），
	  因此您必须使用比论文 150 到 300 毫秒更大的选举超时时间，但请不要太大，因为那可能导致无法在测试要求的 5 秒内选出 Leader。
	*/

	// election timeout
	// MoreVoteTime MinVoteTime 定义随机生成投票过期时间范围:(MoreVoteTime+MinVoteTime~MinVoteTime)
	MoreVoteTime = 200
	MinVoteTime  = 150

	//heartbeat timeout
	// HeartbeatSleep 心脏休眠时间,要注意的是，这个时间要比选举低，才能建立稳定心跳机制
	HeartbeatSleep = 10
	//commit timeout
	CommitSleep = 10
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 所有服务器，持久化状态（lab-2A不要求持久化）
	currentTerm       int        // * 见过的最大任期
	votedFor          int        // 记录在currentTerm任期投票给谁了
	log               []LogEntry // 操作日志
	lastIncludedIndex int        // snapshot最后1个logEntry的index，没有snapshot则为0
	lastIncludedTerm  int        // snapthost最后1个logEntry的term，没有snaphost则无意义

	// 所有服务器，易失状态(在所有server上经常变的)
	//* apply是写入未提交，commit是提交
	commitIndex int // 已知的最大已提交索引
	lastApplied int // 当前应用到状态机的索引

	// 仅Leader，易失状态（选举成为leader时重置）
	nextIndex  []int //	每个follower的log同步起点索引（初始化为leader log的last log index +1）
	matchIndex []int // 每个follower的log同步进度（初始化为0），和nextIndex强关联

	// 所有服务器，选举相关状态
	role              string    // 身份
	leaderId          int       // leader的id
	lastActiveTime    time.Time // 上次活跃时间（刷新时机：收到leader心跳、给其他candidates投票、请求其他节点投票）
	lastBroadcastTime time.Time // 作为leader，上次的广播时间

	applyCh chan ApplyMsg // 应用层的提交队列
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == ROLE_LEADER
	return term, isleader
}

//------------------------------------------------状态持久化--------------------------------------------------
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// 不用加锁，外层逻辑会锁
	DPrintf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%d]", rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
	data := rf.raftStateForPersist()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) raftStateForPersist() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	// 为了snaphost多做了2个持久化的metadata
	d.Decode(&rf.lastIncludedIndex)
	d.Decode(&rf.lastIncludedTerm)
}

//------------------------------------rpc request args & reply-------------------------------------------
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int // candidate日志条目最后index(2D包含快照)
	LastLogTerm  int // candidate最后日志条目的任期号(2D包含快照)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

//--------------------------------rpc handler 选举、日志同步、install snapshot-----------------------------------

// example RequestVote RPC handler.
// 已经兼容snapshot
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//对竞选rpc的reply的初始化
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	DPrintf("RaftNode[%d] Handle RequestVote, CandidatesId[%d] Term[%d] CurrentTerm[%d] LastLogIndex[%d] LastLogTerm[%d] votedFor[%d]",
		rf.me, args.CandidateId, args.Term, rf.currentTerm, args.LastLogIndex, args.LastLogTerm, rf.votedFor)
	defer func() {
		DPrintf("RaftNode[%d] Return RequestVote, CandidatesId[%d] Term[%d] currentTerm[%d] VoteGranted[%v] ", rf.me, args.CandidateId,
			args.Term, rf.currentTerm, reply.VoteGranted)
	}()

	// 竞选发起者的任期不如接收者大，拒绝投票
	if args.Term < rf.currentTerm {
		return
	}

	// 竞选发起者的任期比接收者大，接收者则转为该任期的follower
	// * 注意发起者和接收者term相等不会被俘获
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.leaderId = -1
		// 继续向下走，进行投票
	}

	// 每个任期，只能投票给1人
	// *接收者投票
	// ** 被当前接收者投票的candidate的日志必须比接收者的新（因为raft的日志同步是leader到follower单向的）
	// 1、发起者最后一条log的term大于接收者的last log term则更新
	// 2、last log term相同, 更长的log则更新
	// 这里坑了好久，一定要严格遵守论文的逻辑，另外log长度一样也是可以给对方投票的
	lastLogTerm := rf.lastTerm()
	//此时 args.Term >= rf.currentTerm
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && //接收者在当前term还没有投票或者已经投给了此candidate
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= rf.lastIndex())) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		// 为其他人投票，那么重置接收者的下次投票时间
		rf.lastActiveTime = time.Now()
	}

	rf.persist()
}

// 已兼容snapshot
//log append rpc handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("RaftNode[%d] Handle AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s] logIndex[%d] prevLogIndex[%d] prevLogTerm[%d] commitIndex[%d] Entries[%v]",
		rf.me, rf.leaderId, args.Term, rf.currentTerm, rf.role, rf.lastIndex(), args.PrevLogIndex, args.PrevLogTerm, rf.commitIndex, args.Entries)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	defer func() {
		DPrintf("RaftNode[%d] Return AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s] logIndex[%d] prevLogIndex[%d] prevLogTerm[%d] Success[%v] commitIndex[%d] log[%v] ConflictIndex[%d]",
			rf.me, rf.leaderId, args.Term, rf.currentTerm, rf.role, rf.lastIndex(), args.PrevLogIndex, args.PrevLogTerm, reply.Success, rf.commitIndex, len(rf.log), reply.ConflictIndex)
	}()

	//发起者的term必须大于接收者的term
	//如果 term < currentTerm 就返回 false （5.1 节）
	if args.Term < rf.currentTerm {
		return
	}

	// 接收者发现发起者有更大的任期，则转为该任期的follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.persist()
		// 继续向下走
	}

	// 认识新的leader
	rf.leaderId = args.LeaderId
	// 刷新活跃时间
	rf.lastActiveTime = time.Now()

	/*
	  首先看一下prevLogIndex处是否有本地日志（prevLogIndex==0除外，相当于从头同步日志），
	  没有的话则还需要leader来继续回退nextIndex直到prevLogIndex位置有日志。在prevLogIndex有日志的前提下，
	  还需要进一步判断prevLogIndex位置的Term是否一样。
	*/
	// 如果prevLogIndex在快照内，且不是快照最后一个log，那么只能从index=1开始同步了
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.ConflictIndex = 1
		return
	} else if args.PrevLogIndex == rf.lastIncludedIndex { // prevLogIndex正好等于快照的最后一个log
		if args.PrevLogTerm != rf.lastIncludedTerm { // 冲突了，那么从index=1开始同步吧
			reply.ConflictIndex = 1
			return
		}
		// prevLogIndex在快照之后，那么进一步判定
	} else {
		if args.PrevLogIndex > rf.lastIndex() { // prevLogIndex位置没有日志的case
			reply.ConflictIndex = rf.lastIndex() + 1
			return
		}
		// prevLogIndex位置有日志，那么判断term必须相同，否则false
		// 如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配，则返回 false （5.3 节）
		if rf.log[rf.index2LogPos(args.PrevLogIndex)].Term != args.PrevLogTerm {
			reply.ConflictTerm = rf.log[rf.index2LogPos(args.PrevLogIndex)].Term
			// * 找到冲突term的在follower已有log中首次出现位置，即截断follower在此term的所有日志
			for index := rf.lastIncludedIndex + 1; index <= args.PrevLogIndex; index++ {
				if rf.log[rf.index2LogPos(index)].Term == reply.ConflictTerm {
					reply.ConflictIndex = index
					break
				}
			}
			return
		}

		//如果没有日志冲突(经过上面的判定都没有return)，走后续的日志覆盖逻辑
	}

	// 保存日志
	for i, logEntry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		logPos := rf.index2LogPos(index)
		if index > rf.lastIndex() { // 已经超出现有日志长度，继续追加
			rf.log = append(rf.log, logEntry)
		} else { // 在重叠部分时，做判断是否覆盖，一旦发生同index log的term冲突，则放弃后续args传来的的append日志同步
			if rf.log[logPos].Term != logEntry.Term {
				rf.log = rf.log[:logPos]          // 删除当前以及后续所有log
				rf.log = append(rf.log, logEntry) // 把新log加入进来
			} // term一样啥也不用做，继续向后比对Log
		}
	}
	rf.persist()

	// 更新slave的commit index
	// 如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值 中较小的一个
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.lastIndex() < rf.commitIndex {
			rf.commitIndex = rf.lastIndex()
		}
	}
	reply.Success = true
}

// 安装快照RPC Handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("RaftNode[%d] installSnapshot starts, rf.lastIncludedIndex[%d] rf.lastIncludedTerm[%d] args.lastIncludedIndex[%d] args.lastIncludedTerm[%d] logSize[%d]",
		rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, args.LastIncludedIndex, args.LastIncludedTerm, len(rf.log))

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	// 发现更大的任期，则转为该任期的follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.persist()
		// 继续向下走
	}

	// 认识新的leader
	rf.leaderId = args.LeaderId
	// 刷新活跃时间
	rf.lastActiveTime = time.Now()

	// leader快照不如本地长，那么忽略这个快照
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	} else { // leader快照比本地快照长
		if args.LastIncludedIndex < rf.lastIndex() { // 快照外还有日志，判断是否需要截断
			if rf.log[rf.index2LogPos(args.LastIncludedIndex)].Term != args.LastIncludedTerm {
				rf.log = make([]LogEntry, 0) // term冲突，扔掉快照外的所有日志
			} else { // term没冲突，保留后续日志
				leftLog := make([]LogEntry, rf.lastIndex()-args.LastIncludedIndex)
				copy(leftLog, rf.log[rf.index2LogPos(args.LastIncludedIndex)+1:])
				rf.log = leftLog
			}
		} else {
			rf.log = make([]LogEntry, 0) // 快照比本地日志长，日志就清空了
		}
	}
	// 更新快照位置
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	// 持久化raft state和snapshot
	rf.persister.SaveStateAndSnapshot(rf.raftStateForPersist(), args.Data)
	// snapshot提交给应用层
	rf.installSnapshotToApplication()
	DPrintf("RaftNode[%d] installSnapshot ends, rf.lastIncludedIndex[%d] rf.lastIncludedTerm[%d] args.lastIncludedIndex[%d] args.lastIncludedTerm[%d] logSize[%d]",
		rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, args.LastIncludedIndex, args.LastIncludedTerm, len(rf.log))
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.

// the first return value is the index that the command will appear at if it's ever committed.
// the second return value is the current term.
// the third return value is true if this server believes it is the leader.

// there is no guarantee that this command will ever be committed to the Raft log,
// since the leader may fail or lose an election.
// 不保证command一定被写到leader的log，因为leader可能会被其他的leader俘获
// 已兼容snapshot
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 只有leader才能写入
	if rf.role != ROLE_LEADER {
		return -1, -1, false
	}

	logEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, logEntry)
	//index = rf.lastIndex()
	// * 这里应该是last log index +1 吧
	index = rf.lastIndex() + 1
	term = rf.currentTerm
	rf.persist()

	DPrintf("RaftNode[%d] Add Command, logIndex[%d] currentTerm[%d]", rf.me, index, term)
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//-------------------------------------------ticker------------------------------------
// 已经兼容snapshot
//选举 ticker  含rpc call
func (rf *Raft) electionLoop() {
	for !rf.killed() {
		//time.Sleep(10 * time.Millisecond)
		now := time.Now()
		/*
			To convert an integer number of units to a Duration, multiply:
			seconds := 10
			fmt.Print(time.Duration(seconds)*time.Second) // prints 10s
		*/
		//这里也解决了一个问题，就是在竞选时，多个候选人得票一样，那么就重新竞选，因为sleep的时间是随机的，那么再此得票相同的概率就极小
		// 超时随机化
		//timeout := time.Duration(MinVoteTime+rand.Int31n(MoreVoteTime)) * time.Millisecond
		time.Sleep(time.Duration(MinVoteTime+rand.Int31n(MoreVoteTime)) * time.Millisecond)

		//这里的匿名函数为了圈定加锁和放锁的范围
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			//elapses := now.Sub(rf.lastActiveTime)

			//* 此处的逻辑为每次sleep之前now是晚于lastActiveTime的，在electionLoop sleep期间，slave收到心跳、成功投票会更新votedTimer,成功install snapshot
			//  master在竞选成功后会更新lastActiveTime
			//  那么sleep醒后如果lastActiveTime仍然小于在sleep睡眠之前定义的时间now，则代表需要发起选举
			// follower -> candidates
			//if rf.role != ROLE_LEADER && elapses >= timeout {
			if rf.role != ROLE_LEADER && rf.lastActiveTime.Before(now) {
				// follower -> candidates
				if rf.role == ROLE_FOLLOWER {
					DPrintf("RaftNode[%d] Follower -> Candidate", rf.me)
					rf.role = ROLE_CANDIDATES
				}

				rf.lastActiveTime = now // 重置上次活跃时间
				rf.currentTerm += 1     // 发起新任期
				rf.votedFor = rf.me     // 该任期投了自己
				rf.persist()

				// 请求投票req
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.lastIndex(),
					LastLogTerm:  rf.lastTerm(),
				}

				//* 在rpc之前释放锁，因为rpc这里的是同步调用，同时注意在rpc之后的状态二次判断
				rf.mu.Unlock()

				DPrintf("RaftNode[%d] RequestVote starts, Term[%d] LastLogIndex[%d] LastLogTerm[%d]", rf.me, args.Term,
					args.LastLogIndex, args.LastLogTerm)

				// 并发RPC请求vote
				type VoteResult struct {
					peerId int
					resp   *RequestVoteReply
				}

				// 收到投票个数（先给自己投1票）
				voteCount := 1
				// 收到应答个数
				finishCount := 1
				voteResultChan := make(chan *VoteResult, len(rf.peers))

				for peerId := 0; peerId < len(rf.peers); peerId++ {
					//异步发起选举
					//* 这里异步rpc call，也是为了当前协程尽快进入下面的select等待reply结果
					go func(id int) {
						if id == rf.me {
							return
						}
						resp := RequestVoteReply{}
						if ok := rf.sendRequestVote(id, &args, &resp); ok {
							voteResultChan <- &VoteResult{peerId: id, resp: &resp}
						} else {
							voteResultChan <- &VoteResult{peerId: id, resp: nil}
						}
					}(peerId)
				}

				maxTerm := 0

				//无限for循环和select的联合使用
				//* 这里使用channel进行投票结果返回而不是rpc的reply，是否有在获取过半票数后快速返回的考虑？（开销）
				// channel实现了解耦，同步控制更方便
				for {
					select {
					case voteResult := <-voteResultChan:
						finishCount += 1
						if voteResult.resp != nil {
							if voteResult.resp.VoteGranted {
								voteCount += 1
							}
							//maxTerm 记录所有peers的竞选rpc reply中最大的接收者term
							if voteResult.resp.Term > maxTerm {
								maxTerm = voteResult.resp.Term
							}
						}
						// 当获取到本轮的全部reply后或者中途得票过半，立即离开
						if finishCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
							goto VOTE_END
						}
					}
				}

			VOTE_END:
				//* 在rpc调用前释放了锁，这里要再加回来，因为需要检查server的角色
				rf.mu.Lock()
				defer func() {
					DPrintf("RaftNode[%d] RequestVote ends, finishCount[%d] voteCount[%d] Role[%s] maxTerm[%d] currentTerm[%d]", rf.me, finishCount, voteCount,
						rf.role, maxTerm, rf.currentTerm)
				}()

				// * rpc调用结束后的状态二次判定
				// 如果投票发起者角色改变了，则忽略本轮投票结果
				if rf.role != ROLE_CANDIDATES {
					return
				}

				// response 判断任期
				//* 发现了更高的任期，发起者切回follower，注意发起者的term变为了更大的reply中的term（可能由于网络分区引起）
				if maxTerm > rf.currentTerm {
					rf.role = ROLE_FOLLOWER
					rf.leaderId = -1
					rf.currentTerm = maxTerm
					rf.votedFor = -1
					rf.persist()
					return
				}
				// 赢得大多数选票，则成为leader
				if voteCount > len(rf.peers)/2 {
					rf.role = ROLE_LEADER
					rf.leaderId = rf.me

					rf.nextIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = rf.lastIndex() + 1
					}
					rf.matchIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.matchIndex[i] = 0
					}

					// 令appendEntries广播立即执行
					// Unix()返回一个Time类型, 然后这个时间是UTC时间(UTC中自1970年1月1日起的Unix时间有关)加上你的参数时间
					// 第一个参数是秒，第二个是纳秒
					// * 这里的意思是把lastBroadcastTime变为"最小"，然后尽快触发广播
					// * 防止更多的其他的follower又会成为candidate
					rf.lastBroadcastTime = time.Unix(0, 0)
					return
				}
			}
		}()
	}
}

// lab-2A只做心跳，不考虑log同步
// 日志同步ticker  含rpc call
func (rf *Raft) appendEntriesLoop() {
	for !rf.killed() {
		//HeartbeatSleep = 10ms
		time.Sleep(HeartbeatSleep * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 只有leader才向外广播心跳、日志同步
			if rf.role != ROLE_LEADER {
				return
			}

			// 100ms广播1次
			now := time.Now()
			if now.Sub(rf.lastBroadcastTime) < 100*time.Millisecond {
				return
			}
			rf.lastBroadcastTime = time.Now()

			// 向所有follower发送心跳
			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if peerId == rf.me {
					continue
				}

				// 如果nextIndex在leader的snapshot内，那么直接同步snapshot
				if rf.nextIndex[peerId] <= rf.lastIncludedIndex {
					rf.doInstallSnapshot(peerId)
				} else { // 否则同步日志
					rf.doAppendEntries(peerId)
				}
			}
		}()
	}
}

// 已兼容snapshot
// append ticker 实际调用的同步日志操作
// 进入此函数的前提 rf.nextIndex[peerId] > rf.lastIncludedIndex
func (rf *Raft) doAppendEntries(peerId int) {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	//空[]entry 表示是心跳包
	args.Entries = make([]LogEntry, 0)
	args.PrevLogIndex = rf.nextIndex[peerId] - 1

	//* 构造append log
	// 如果prevLogIndex是leader快照的最后1条log, 那么取快照的最后1个term
	if args.PrevLogIndex == rf.lastIncludedIndex {
		args.PrevLogTerm = rf.lastIncludedTerm
	} else { // 否则一定是log部分
		args.PrevLogTerm = rf.log[rf.index2LogPos(args.PrevLogIndex)].Term
	}
	args.Entries = append(args.Entries, rf.log[rf.index2LogPos(args.PrevLogIndex+1):]...)

	DPrintf("RaftNode[%d] appendEntries starts,  currentTerm[%d] peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] args.Entries[%d] commitIndex[%d]",
		rf.me, rf.currentTerm, peerId, rf.lastIndex(), rf.nextIndex[peerId], rf.matchIndex[peerId], len(args.Entries), rf.commitIndex)

	//异步rpc call
	go func() {
		// DPrintf("RaftNode[%d] appendEntries starts, myTerm[%d] peerId[%d]", rf.me, args1.Term, id)
		reply := AppendEntriesReply{}

		if ok := rf.sendAppendEntries(peerId, &args, &reply); ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			defer func() {
				DPrintf("RaftNode[%d] appendEntries ends,  currentTerm[%d]  peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] commitIndex[%d]",
					rf.me, rf.currentTerm, peerId, rf.lastIndex(), rf.nextIndex[peerId], rf.matchIndex[peerId], rf.commitIndex)
			}()

			// 因为rpc没有锁，所以在rpc返回之后要进行状态的二次判定
			// 如果不是rpc前的leader状态了，那么啥也别做了
			if rf.currentTerm != args.Term {
				return
			}
			// 变成follower
			if reply.Term > rf.currentTerm {
				rf.role = ROLE_FOLLOWER
				rf.leaderId = -1
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				return
			}

			// 因为RPC期间无锁, 可能相关状态被其他RPC修改了
			// 因此这里得根据发出RPC请求时的状态做更新，而不要直接对nextIndex和matchIndex做相对加减
			if reply.Success { // 同步日志成功,没有prevLogIndex冲突
				rf.nextIndex[peerId] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1

				// 在每次matchIndex[]更新后，尝试更新leader的commitIndex
				rf.updateCommitIndex()
			} else { //有日志冲突，需要leader进行日志回退（回退nextIndex）
				// 回退优化，参考：https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
				/*
				  优化之前的方式是slave发生log冲突后，只返回ConflictIndex，然后leader把nextIndex[slaveId]改为传回的ConflictIndex，相当于回退了1
				  优化后的方式是slave传回confictIndex和confictTerm，然后在leader log中找在本次rpc call的prevLogIndex之前的confictTerm的
				  最晚出现位置作为该peer server的nextIndex。
				  *  *
				  *  *
				  我觉得这里有问题，上面的参考链接里说的是冲突后，follower回退conflictTerm的所有log，返回第一个conflictTerm所在的
				*/
				// 回退前的旧nextIndex
				nextIndexBefore := rf.nextIndex[peerId] // 仅为打印log

				//同步日志失败，出现日志冲突
				// follower的prevLogIndex位置term与发起者的prevLogTerm冲突了
				if reply.ConflictTerm != -1 {
					// 我们找leader log中在preLogIndex之前的conflictTerm最后出现位置，如果找到了就用它作为nextIndex，否则用follower的conflictIndex
					conflictTermIndex := -1
					for index := args.PrevLogIndex; index > rf.lastIncludedIndex; index-- { // 找最后一个conflictTerm
						if rf.log[rf.index2LogPos(index)].Term == reply.ConflictTerm {
							conflictTermIndex = index
							break
						}
					}
					// leader也存在冲突term的日志，则从term最后一次出现之后的日志开始尝试同步，因为leader/follower可能在该term的日志有部分相同
					if conflictTermIndex != -1 {
						rf.nextIndex[peerId] = conflictTermIndex
					} else {
						// leader并没有term的日志，那么把follower日志中该term首次出现的位置作为尝试同步的位置，即截断follower在此term的所有日志
						// 用follower首次出现term的index作为同步开始
						rf.nextIndex[peerId] = reply.ConflictIndex
					}
				} else {
					// follower没有发现prevLogIndex term冲突, 可能是被snapshot了或者日志长度不够
					// 这时候我们将返回的conflictIndex设置为nextIndex即可
					rf.nextIndex[peerId] = reply.ConflictIndex
				}
				DPrintf("RaftNode[%d] back-off nextIndex, peer[%d] nextIndexBefore[%d] nextIndex[%d]", rf.me, peerId, nextIndexBefore, rf.nextIndex[peerId])
			}
		}
	}()
}

// append ticker实际调用的 同步snapshot 操作
func (rf *Raft) doInstallSnapshot(peerId int) {
	DPrintf("RaftNode[%d] doInstallSnapshot starts, leaderId[%d] peerId[%d]\n", rf.me, rf.leaderId, peerId)

	args := InstallSnapshotArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.lastIncludedIndex
	args.LastIncludedTerm = rf.lastIncludedTerm
	args.Offset = 0
	args.Data = rf.persister.ReadSnapshot()
	args.Done = true

	reply := InstallSnapshotReply{}

	go func() {
		if rf.sendInstallSnapshot(peerId, &args, &reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 如果不是rpc前的leader状态了，那么啥也别做了
			if rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm { // 变成follower
				rf.role = ROLE_FOLLOWER
				rf.leaderId = -1
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				return
			}
			rf.nextIndex[peerId] = rf.lastIndex() + 1      // 重新从末尾同步log（未经优化，但够用）
			rf.matchIndex[peerId] = args.LastIncludedIndex // 已同步到的位置（未经优化，但够用）
			rf.updateCommitIndex()                         // 更新commitIndex
			DPrintf("RaftNode[%d] doInstallSnapshot ends, leaderId[%d] peerId[%d] nextIndex[%d] matchIndex[%d] commitIndex[%d]\n", rf.me, rf.leaderId, peerId, rf.nextIndex[peerId],
				rf.matchIndex[peerId], rf.commitIndex)
		}
	}()
}

// 1. ApplyMsg
//    each time a new entry is committed to the log, each Raft peer
//    should send an ApplyMsg to the service (or tester)
//    in the same server.
// 2. applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages.

//日志提交
//应该是一个异步协程的log commit的补偿操作
//在函数updateCommitIndex中commitIndex已经更新了，此时commitIndex大于了AppliesIndex，但当时不执行要求的ApplyMsg的发送
//commitLogLoop就是异步完成ApplyMsg的发送，然后让lastApplied慢慢追上commitIndex
func (rf *Raft) commitLogLoop() {
	noMore := false
	for !rf.killed() {
		if noMore {
			time.Sleep(CommitSleep * time.Millisecond)
		}
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			noMore = true

			//如果有未提交的log
			//每次提交一条log
			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				appliedIndex := rf.index2LogPos(rf.lastApplied)
				appliedMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[appliedIndex].Command,
					CommandIndex: rf.lastApplied,
					CommandTerm:  rf.log[appliedIndex].Term,
				}
				rf.applyCh <- appliedMsg // 引入snapshot后，这里必须在锁内投递了，否则会和snapshot的交错产生bug
				DPrintf("RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d]", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
				noMore = false
			}
		}()
	}
}

//-----------------------------------snapshot 相关方法-----------------------------------------

func (rf *Raft) installSnapshotToApplication() {
	var applyMsg *ApplyMsg

	// 同步给application层的快照
	applyMsg = &ApplyMsg{
		CommandValid:      false,
		Snapshot:          rf.persister.ReadSnapshot(),
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
	}
	// 快照部分就已经提交给application了，所以后续applyLoop提交日志后移
	rf.lastApplied = rf.lastIncludedIndex

	DPrintf("RaftNode[%d] installSnapshotToApplication, snapshotSize[%d] lastIncludedIndex[%d] lastIncludedTerm[%d]",
		rf.me, len(applyMsg.Snapshot), applyMsg.LastIncludedIndex, applyMsg.LastIncludedTerm)
	rf.applyCh <- *applyMsg
	return
}

// 保存snapshot，截断log
func (rf *Raft) TakeSnapshot(snapshot []byte, lastIncludedIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 已经有更大index的snapshot了
	if lastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	// 快照的当前元信息
	DPrintf("RafeNode[%d] TakeSnapshot begins, IsLeader[%v] snapshotLastIndex[%d] lastIncludedIndex[%d] lastIncludedTerm[%d]",
		rf.me, rf.leaderId == rf.me, lastIncludedIndex, rf.lastIncludedIndex, rf.lastIncludedTerm)

	// 要压缩的日志长度
	compactLogLen := lastIncludedIndex - rf.lastIncludedIndex

	// 更新快照元信息
	rf.lastIncludedTerm = rf.log[rf.index2LogPos(lastIncludedIndex)].Term
	rf.lastIncludedIndex = lastIncludedIndex

	// 压缩日志
	afterLog := make([]LogEntry, len(rf.log)-compactLogLen)
	copy(afterLog, rf.log[compactLogLen:])
	rf.log = afterLog

	// 把snapshot和raftstate持久化
	rf.persister.SaveStateAndSnapshot(rf.raftStateForPersist(), snapshot)

	DPrintf("RafeNode[%d] TakeSnapshot ends, IsLeader[%v] snapshotLastIndex[%d] lastIncludedIndex[%d] lastIncludedTerm[%d]",
		rf.me, rf.leaderId == rf.me, lastIncludedIndex, rf.lastIncludedIndex, rf.lastIncludedTerm)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order.

// persister is a place for this server to save its persistent state,
// and also initially holds the most recent saved state, if any.

// applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages.

// Make() must return quickly, so it should start goroutines for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = ROLE_FOLLOWER
	rf.leaderId = -1
	rf.votedFor = -1
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.lastActiveTime = time.Now()
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 向application层安装快照
	rf.installSnapshotToApplication()

	DPrintf("RaftNode[%d] Make again", rf.me)

	// election逻辑
	go rf.electionLoop()
	// leader逻辑
	go rf.appendEntriesLoop()
	// apply逻辑
	go rf.commitLogLoop()

	DPrintf("Raftnode[%d]启动", me)

	return rf
}
