package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"bytes"

	//	"6.824/labgob"
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

const ELECTION_TIMEOUT = 500 * time.Millisecond             //选举超时时间
const HEARTBEAT_TIMEOUT = 300 * time.Millisecond            //心跳超时时间
const HEARTBEAT_INTERVAL = 100 * time.Millisecond           //心跳间隔时间
const LEADER_TIMEOUT_CHECK_INTERVAL = 30 * time.Millisecond //领导者超时检测间隔时间
const RPC_TIMEOUT_TIME = 50 * time.Millisecond              //RPC调用超时时间

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

	isLeader bool //是否是Leader

	//所有服务器上的持久性状态 (在响应 RPC 请求之前，已经更新到了稳定的存储设备)
	currentTerm int   //服务器已知最新的任期（在服务器首次启动时初始化为0，单调递增）
	votedFor    int   //当前任期内收到选票的 candidateId，如果没有投给任何候选人 则为空
	log         []Log //日志条目；每个条目包含了用于状态机的命令，以及领导人接收到该条目时的任期（初始索引为1）

	//所有服务器上的易失性状态
	commitIndex int //已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int //已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
	leaderId    int //领导者服务器id
	//领导人（服务器）上的易失性状态 (选举后已经重新初始化)
	nextIndex  []int //对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1）
	matchIndex []int //对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）

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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm { //请求的任期号小于当前任期号，无效请求
		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm { //请求的任期号大于当前任期号，更新自己的任期号，以及投票数据
		rf.updateTerm(args.Term)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else { //检查是否投票
		if rf.votedFor == -1 { //未投票，投给候选人
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else { //已投票，无法投给候选人
			reply.VoteGranted = false
		}
	}
	reply.Term = rf.currentTerm

	//fmt.Printf("服务器%d: 服务器%d 请求我投票，当前任期%d,我投给了%d\n", rf.me, args.CandidateId, rf.currentTerm, rf.votedFor)
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
	Term         int        //领导人的任期
	LeaderId     int        //领导人ID，因此跟随者可以对客户端进行重定向
	PrevLogIndex int        //紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int        //紧邻新日志条目之前的那个日志条目的任期
	Entries      []ApplyMsg //需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int        //领导人的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term    int  //当前任期，对于领导人而言 它会更新自己的任期
	Success bool //如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm { //请求的任期号大于当前任期号
		rf.updateTerm(args.Term)
		rf.lastHeartBeatTime = time.Now().UnixMilli()
		rf.leaderId = args.LeaderId
	} else if args.Term == rf.currentTerm { //请求的任期号等于当前任期号
		rf.lastHeartBeatTime = time.Now().UnixMilli()
		rf.leaderId = args.LeaderId
		rf.isLeader = false
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
				LastLogIndex: len(rf.log),
				LastLogTerm: func() int {
					if len(rf.log) == 0 {
						return 0
					} else {
						return rf.log[len(rf.log)-1].Term
					}
				}(),
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
					rf.isLeader = true
					return true
				}
			}
		}

	}

	return false
}

func (rf *Raft) sendHeartbeats() {
	replys := make([]*AppendEntriesReply, len(rf.peers))
	oks := make([]bool, len(rf.peers))
	isEfficient := true
	var lock sync.Mutex

	for idx := 0; idx < len(rf.peers); idx++ { //向其他所有跟随者发送心跳包
		if idx == rf.me {
			continue
		}

		go func(idx int) {
			args := &AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
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
	for idx := 0; idx < len(rf.peers); idx++ { //向其他所有跟随者发送心跳包
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
			rf.sendHeartbeats()
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
		time.Sleep(sleepTime)

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
	rf.isLeader = false

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Log, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.leaderId = -1

	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)

	rf.lastHeartBeatTime = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) updateTerm(newTerm int) {

	rf.isLeader = false

	//所有服务器上的持久性状态 (在响应 RPC 请求之前，已经更新到了稳定的存储设备)
	rf.currentTerm = newTerm
	rf.votedFor = -1
}
