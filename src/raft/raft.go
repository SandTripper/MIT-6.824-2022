package raft

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"bytes"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

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

const ELECTION_TIMEOUT = 200 * time.Millisecond             //选举超时时间
const HEARTBEAT_TIMEOUT = 300 * time.Millisecond            //心跳超时时间
const HEARTBEAT_INTERVAL = 100 * time.Millisecond           //心跳间隔时间
const LEADER_TIMEOUT_CHECK_INTERVAL = 30 * time.Millisecond //领导者超时检测间隔时间
const RPC_TIMEOUT_TIME = 20 * time.Millisecond              //RPC调用超时时间
const APPENDENTRIES_INTERVAL = 50 * time.Millisecond        //发送AE的间隔时间

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	Command interface{} //命令
	Term    int         //接收到该条目时的任期
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyChannal chan ApplyMsg //提交命令的channal
	isLeader     bool          //是否是Leader

	//所有服务器上的持久性状态 (在响应 RPC 请求之前，已经更新到了稳定的存储设备)
	currentTerm int   //服务器已知最新的任期（在服务器首次启动时初始化为0，单调递增）
	votedFor    int   //当前任期内收到选票的 candidateId，如果没有投给任何候选人 则为空
	log         []Log //日志条目；每个条目包含了用于状态机的命令，以及领导人接收到该条目时的任期（初始索引为1）

	//所有服务器上的易失性状态
	commitIndex int //已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int //已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
	leaderId    int //领导者服务器id
	//领导人（服务器）上的易失性状态 (选举后已经重新初始化)
	nextIndex          []int  //对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1）
	matchIndex         []int  //对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）
	isLastMatchSuccess []bool //对于每一台服务器，上次匹配是否成功（初始值为false）
	totalAccept        []int  //对于每一条日志，被接受的数量

	lastHeartBeatTime int64 //上次接收到心跳包的时间戳，单位：毫秒
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.isLeader
	rf.mu.Unlock()

	return term, isleader
}

type RaftState struct {
	IsLeader           bool   //是否是Leader
	CurrentTerm        int    //服务器已知最新的任期
	VotedFor           int    //当前任期内收到选票的 candidateId
	Log                []Log  //日志条目
	CommitIndex        int    //已知已提交的最高的日志条目的索引
	LastApplied        int    //已经被应用到状态机的最高的日志条目的索引
	LeaderId           int    //领导者服务器id
	NextIndex          []int  //发送到每一台服务器的下一个日志条目的索引
	MatchIndex         []int  //已知已复制到每一台服务器的最高日志条目的索引
	IsLastMatchSuccess []bool //上次匹配是否成功
	TotalAccept        []int  //每一条日志被接受的数量
	LastHeartBeatTime  int64  //上次接收到心跳包的时间戳
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	state := &RaftState{
		IsLeader:           rf.isLeader,
		CurrentTerm:        rf.currentTerm,
		VotedFor:           rf.votedFor,
		Log:                append([]Log(nil), rf.log...),
		CommitIndex:        rf.commitIndex,
		LastApplied:        rf.lastApplied,
		LeaderId:           rf.leaderId,
		NextIndex:          append([]int(nil), rf.nextIndex...),
		MatchIndex:         append([]int(nil), rf.matchIndex...),
		IsLastMatchSuccess: append([]bool(nil), rf.isLastMatchSuccess...),
		TotalAccept:        append([]int(nil), rf.totalAccept...),
		LastHeartBeatTime:  rf.lastHeartBeatTime,
	}

	if err := e.Encode(state); err != nil {
		log.Fatalf("failed to encode state: %v", err)
	}

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	state := &RaftState{}

	if err := d.Decode(state); err != nil {
		log.Fatalf("failed to decode state: %v", err)
	}

	rf.isLeader = state.IsLeader
	rf.currentTerm = state.CurrentTerm
	rf.votedFor = state.VotedFor
	rf.log = state.Log
	rf.commitIndex = state.CommitIndex
	rf.lastApplied = state.LastApplied
	rf.leaderId = state.LeaderId
	rf.nextIndex = state.NextIndex
	rf.matchIndex = state.MatchIndex
	rf.isLastMatchSuccess = state.IsLastMatchSuccess
	rf.totalAccept = state.TotalAccept
	rf.lastHeartBeatTime = state.LastHeartBeatTime
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //候选人的任期号
	CandidateId  int //请求选票的候选人的 ID
	LastLogIndex int //候选人的最后日志条目的索引值
	LastLogTerm  int //候选人最后日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool //候选人赢得了此张选票时为真
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm { //请求的任期号小于当前任期号，无效请求
		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm { //请求的任期号大于当前任期号，更新自己的任期号，进行下一步判断
		rf.updateTerm(args.Term)

		if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1) { //日志合法
			rf.votedFor = args.CandidateId
			//fmt.Printf("server%d: good,from server %d,args.LastLogTerm: %d ,args.LastLogIndex: %d\n", rf.me, args.CandidateId, args.LastLogTerm, args.LastLogIndex)
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}

	} else {
		if rf.votedFor == -1 { //未投票，判断是否符合投票要求
			if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
				args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log) { //日志合法
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
			} else {
				reply.VoteGranted = false
			}
		} else { //已投票，无法投给候选人
			reply.VoteGranted = false
		}
	}
	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int   //领导人的任期
	LeaderId     int   //领导人ID，因此跟随者可以对客户端进行重定向
	PrevLogIndex int   //紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int   //紧邻新日志条目之前的那个日志条目的任期
	Entries      []Log //需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int   //领导人的已知已提交的最高的日志条目的索引
	IsGood       bool  //是否状态一致
}

type AppendEntriesReply struct {
	Term    int  //当前任期，对于领导人而言 它会更新自己的任期
	Success bool //如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
	XTerm   int  //冲突的任期
	XIndex  int  //冲突的任期的第一个索引
	XLen    int  //log的长度
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm { //请求的任期号小于当前任期号，过期的请求
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm { //请求的任期号大于当前任期号，更新当前的任期
		rf.updateTerm(args.Term)
	}

	rf.lastHeartBeatTime = time.Now().UnixMilli()
	rf.leaderId = args.LeaderId
	rf.isLeader = false

	if len(args.Entries) != 0 { //请求带有日志信息，处理日志
		if args.PrevLogIndex > len(rf.log)-1 {
			reply.Success = false
			reply.XLen = len(rf.log)
			reply.XTerm = -1
			reply.XIndex = -1
		} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			reply.XLen = len(rf.log)
			xTerm := rf.log[args.PrevLogIndex].Term
			xIndex := args.PrevLogIndex
			for xIndex != 0 && rf.log[xIndex-1].Term == xTerm {
				xIndex--
			}
			reply.XTerm = xTerm
			reply.XIndex = xIndex
		} else {
			reply.Success = true
			rf.log = rf.log[:args.PrevLogIndex+1] //删除后面的日志
			//fmt.Printf("server%d: add at:%v, len:%v\n", rf.me, args.PrevLogIndex+1, len(args.Entries))
			rf.log = append(rf.log, args.Entries...) //添加日志
		}

	}

	if args.LeaderCommit > rf.commitIndex && args.IsGood { //更新提交索引
		if args.LeaderCommit < len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
	}
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		//fmt.Printf("time:%v, server:%v, submit index:%v, valua:%v\n", time.Now().UnixMilli(), rf.me, rf.lastApplied, rf.log[rf.lastApplied].Command)
		rf.applyChannal <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
	}

	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	isLeader = rf.isLeader
	term = rf.currentTerm
	index = len(rf.log)
	if rf.isLeader {
		rf.log = append(rf.log, Log{
			Command: command,
			Term:    rf.currentTerm,
		})
		rf.totalAccept = append(rf.totalAccept, 1)
	}
	//fmt.Printf("time:%v, server:%v, index: %v, term: %v, isLeader %v, command:%v\n", time.Now().UnixMilli(), rf.me, index, term, isLeader, command)

	// //fmt.Printf("time:%v, server:%v", time.Now().UnixMilli(), rf.me)
	// for _, v := range rf.log {
	// 	//fmt.Printf("  %v", v)
	// }
	// //fmt.Printf("\n")

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 发动新选举
func (rf *Raft) startNewElection() bool {

	rf.currentTerm++    //增加当前任期
	rf.votedFor = rf.me //自己先投自己
	myVoteNum := 1      //当前获得的选票

	replys := make([]*RequestVoteReply, len(rf.peers))
	oks := make([]bool, len(rf.peers))
	isEfficient := true
	var lock sync.Mutex

	for idx := 0; idx < len(rf.peers); idx++ { //向其他所有服务器请求投自己

		if idx == rf.me {
			continue
		}

		go func(idx int) {
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			reply := &RequestVoteReply{}

			ok := rf.sendRequestVote(idx, args, reply)

			if !isEfficient {
				return
			}

			lock.Lock()
			oks[idx] = ok
			replys[idx] = reply
			lock.Unlock()

		}(idx)
	}

	time.Sleep(RPC_TIMEOUT_TIME)

	isEfficient = false

	for idx := 0; idx < len(rf.peers); idx++ { //检查请求结果
		if idx == rf.me {
			continue
		}

		if oks[idx] {
			if replys[idx].Term > rf.currentTerm { //当前任期已过时
				rf.updateTerm(replys[idx].Term)
				return false
			}
			if replys[idx].VoteGranted {

				myVoteNum++

				if myVoteNum >= len(rf.peers)/2+1 { //获得多数选票,成为领导者
					rf.becomeLeader()
					return true
				}
			}
		}

	}

	return false
}

func (rf *Raft) sendHeartBeatToFollowers() {
	replys := make([]*AppendEntriesReply, len(rf.peers))
	oks := make([]bool, len(rf.peers))
	isEfficient := true
	var lock sync.Mutex

	for idx := 0; idx < len(rf.peers); idx++ { //向其他所有跟随者发送
		if idx == rf.me {
			continue
		}

		go func(idx int) {
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				IsGood:       rf.matchIndex[idx] >= len(rf.log)-1,
			}

			reply := &AppendEntriesReply{}

			ok := rf.sendAppendEntries(idx, args, reply)
			if !isEfficient {
				return
			}
			lock.Lock()
			replys[idx] = reply
			oks[idx] = ok
			lock.Unlock()

		}(idx)
	}

	time.Sleep(RPC_TIMEOUT_TIME)

	isEfficient = false

	for idx := 0; idx < len(rf.peers); idx++ { //处理回复
		if idx == rf.me {
			continue
		}
		if oks[idx] {
			if replys[idx].Term > rf.currentTerm {
				rf.currentTerm = replys[idx].Term
				rf.isLeader = false
				return
			}
		}
	}
}

func (rf *Raft) sendAEToFollowers() {

	replys := make([]*AppendEntriesReply, len(rf.peers))
	oks := make([]bool, len(rf.peers))
	isEfficient := true
	var lock sync.Mutex

	for idx := 0; idx < len(rf.peers); idx++ { //向其他所有跟随者发送
		if idx == rf.me {
			continue
		}

		if rf.nextIndex[idx] > len(rf.log)-1 || rf.nextIndex[idx] < 1 {
			oks[idx] = false
			continue
		}
		go func(idx int, willPutAllLog bool) {
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: rf.nextIndex[idx] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[idx]-1].Term,
				IsGood:       rf.matchIndex[idx] >= len(rf.log)-1,
			}

			if willPutAllLog {
				args.Entries = rf.log[rf.nextIndex[idx]:]
			} else {
				args.Entries = rf.log[rf.nextIndex[idx] : rf.nextIndex[idx]+1]
			}

			reply := &AppendEntriesReply{}

			ok := rf.sendAppendEntries(idx, args, reply)

			if !isEfficient {
				return
			}

			lock.Lock()
			replys[idx] = reply
			oks[idx] = ok
			lock.Unlock()

		}(idx, rf.isLastMatchSuccess[idx])
	}

	time.Sleep(RPC_TIMEOUT_TIME)

	isEfficient = false
	for idx := 0; idx < len(rf.peers); idx++ { //处理回复
		if idx == rf.me {
			continue
		}
		if oks[idx] {
			if replys[idx].Term > rf.currentTerm {
				rf.currentTerm = replys[idx].Term
				rf.isLeader = false
				return
			}
			//处理日志回复
			if replys[idx].Success {

				if rf.isLastMatchSuccess[idx] {
					for rf.nextIndex[idx] < len(rf.log) {
						rf.totalAccept[rf.nextIndex[idx]]++
						rf.nextIndex[idx]++
					}
					rf.matchIndex[idx] = len(rf.log) - 1
					rf.nextIndex[idx] = len(rf.log)
				} else {
					rf.totalAccept[rf.nextIndex[idx]]++
					rf.matchIndex[idx] = rf.nextIndex[idx]
					rf.nextIndex[idx]++
				}

				for i := rf.commitIndex + 1; i < len(rf.totalAccept); i++ {
					if rf.totalAccept[i] >= len(rf.peers)/2+1 && rf.log[i].Term == rf.currentTerm {
						rf.commitIndex = i
					}
				}

				// for rf.commitIndex < len(rf.totalAccept)-1 && rf.totalAccept[rf.commitIndex+1] >= len(rf.peers)/2+1 {
				// 	rf.commitIndex++
				// 	//fmt.Printf("server:%v, commitIndex:%v\n", rf.me, rf.commitIndex)
				// }

				for rf.lastApplied < rf.commitIndex {
					rf.lastApplied++
					//fmt.Printf("time:%v, server:%v, submit index:%v, value:%v\n", time.Now().UnixMilli(), rf.me, rf.lastApplied, rf.log[rf.lastApplied].Command)
					rf.applyChannal <- ApplyMsg{
						CommandValid: true,
						Command:      rf.log[rf.lastApplied].Command,
						CommandIndex: rf.lastApplied,
					}
				}

				rf.isLastMatchSuccess[idx] = true
			} else {
				rf.isLastMatchSuccess[idx] = false
				if replys[idx].XLen < rf.nextIndex[idx] {
					rf.nextIndex[idx] = replys[idx].XLen
				} else if replys[idx].XTerm != -1 {
					//手写二分查找
					l := 0
					r := rf.nextIndex[idx] - 1
					ans := -1
					for l <= r {
						mid := (l + r) / 2
						if rf.log[mid].Term > replys[idx].XTerm {
							r = mid - 1
						} else if rf.log[mid].Term < replys[idx].XTerm {
							l = mid + 1
						} else {
							ans = mid
							r = mid - 1
						}
					}

					if ans == -1 { //没找到
						rf.nextIndex[idx] = replys[idx].XIndex
					} else {
						rf.nextIndex[idx] = ans
					}
				} else {
					rf.nextIndex[idx]--
				}
			}

		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	var sleepTime time.Duration
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.isLeader {
			rf.sendHeartBeatToFollowers()
			sleepTime = HEARTBEAT_INTERVAL
		} else {
			if time.Now().UnixMilli()-rf.lastHeartBeatTime > HEARTBEAT_TIMEOUT.Milliseconds() { //领导者心跳超时，发动新选举
				if rf.startNewElection() {
					sleepTime = 0
				} else {
					sleepTime = time.Duration(rand.Intn(200)+int(ELECTION_TIMEOUT.Milliseconds())) * time.Millisecond
				}
			} else {
				sleepTime = LEADER_TIMEOUT_CHECK_INTERVAL
			}
		}
		rf.mu.Unlock()
		rf.persist()
		time.Sleep(sleepTime)

	}
}

func (rf *Raft) ticker2() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.isLeader {
			rf.sendAEToFollowers()
		}
		rf.mu.Unlock()
		time.Sleep(APPENDENTRIES_INTERVAL)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0

	// Your initialization code here (2A, 2B, 2C).
	rf.applyChannal = applyCh

	rf.isLeader = false

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Log, 1)
	rf.totalAccept = make([]int, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.leaderId = -1

	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = 1
	}

	rf.matchIndex = make([]int, len(rf.peers))

	rf.isLastMatchSuccess = make([]bool, len(rf.peers))

	rf.lastHeartBeatTime = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.ticker2()
	return rf
}

func (rf *Raft) updateTerm(newTerm int) {

	rf.isLeader = false

	//所有服务器上的持久性状态 (在响应 RPC 请求之前，已经更新到了稳定的存储设备)
	rf.currentTerm = newTerm
	rf.votedFor = -1
}

// 变成领导者
func (rf *Raft) becomeLeader() {

	rf.isLeader = true
	rf.leaderId = rf.me

	rf.totalAccept = make([]int, len(rf.log))

	for i := 0; i < len(rf.totalAccept); i++ {
		rf.totalAccept[i] = 1
	}

	defaultNextIndexValue := len(rf.log)
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = defaultNextIndexValue
	}

	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0
	}

	for i := 0; i < len(rf.matchIndex); i++ {
		rf.isLastMatchSuccess[i] = false
	}
}
