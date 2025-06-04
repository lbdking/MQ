package raft

import (
	"MQ/pb"
	"go.uber.org/zap"
	"math/rand"
	"strconv"
)

type RaftState int

const (
	CandidateState RaftState = iota
	FollowerState
	LeaderState
)

type Raft struct {
	id                    uint64
	state                 RaftState             //节点类型
	leader                uint64                //leader节点id
	currentTerm           uint64                //当前任期
	voteFor               uint64                //投票对象
	raftlog               *RaftLog              //日志
	cluster               *Cluster              //集群节点
	electionTimeout       int                   //选举周期
	heartbeatTimeout      int                   //心跳周期
	randomElectionTimeout int                   //随机选举周期
	electtionTick         int                   //选举计时器
	Tick                  func()                //时钟函数，Leader为心跳时钟，其余节点为选举时钟
	handleMessage         func(*pb.RaftMessage) //消息处理函数
	Msg                   []*pb.RaftMessage     //消息队列
	//ReadIndex             []*ReadIndexResp      //检查Leader完成的readindex
	logger *zap.SugaredLogger
}

func (r *Raft) TickElection() {
	/*- 每次tick选举时钟+1
	- 当选举时钟大于选举周期
	  - follower变为canidate
	  - canidate重新进行选举*/
	r.electtionTick++
	//时钟大于随机选举周期，时钟置0,然后根据当前节点身份执行不同方法
	if r.electtionTick > r.randomElectionTimeout {
		r.electionTimeout = 0
		if r.state == CandidateState {
			r.BroadcastRequestVote()
		}
		if r.state == FollowerState {
			r.SwitchCandidate()
		}
	}
}

func (r *Raft) SwitchCandidate() {
	/*
		- 节点状态变为Candidate,更改消息处理函数为Canidate
		- 重置选举时钟
		- 节点任期+1
		- 节点投票对象为自身
		- 节点发送投票请求
	*/
	r.state = CandidateState
	r.leader = 0
	//随机选举周期等于在基础选举周期上+上一个基础选举周期之内的随机数
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Tick = r.TickElection
	r.handleMessage = r.handleCandidateMessage

	//广播请求，发起选举
	r.BroadcastRequestVote()
	r.electionTimeout = 0

	r.logger.Debugf("候选者，任期 %d , 选举周期 %d s", r.currentTerm, r.randomElectionTimeout)
}

func (r *Raft) handleCandidateMessage(message *pb.RaftMessage) {
	//TOdo
	return
}

// 广播选取投票
func (r *Raft) BroadcastRequestVote() {
	r.currentTerm++
	r.voteFor = r.id
	r.cluster.ResetVoteResult()
	r.cluster.Vote(r.id, true)

	r.logger.Infof("%s 发起投票,", strconv.FormatUint(r.id, 16))

	r.cluster.Foreach(func(id uint64, p *ReplicaProgress) {
		if id == r.id {
			return
		}
		lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()
		r.send(&pb.RaftMessage{
			Type:         pb.MessageType_VOTE,
			Term:         r.currentTerm,
			From:         r.id,
			To:           id,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		})
	})
}

// 将数据添加到消息切片
func (r *Raft) send(msg *pb.RaftMessage) {
	r.Msg = append(r.Msg, msg)
}

// follower,canidate处理投票请求
func (r *Raft) ReciveRequestVote(mTerm, mCandidateId, nLAstLogTerm, mLastLogIndex uint64) (success bool) {
	/*
	 -当前任期未投票，请求方的最新日志大于自身，则同意
	 -当前任期已投票，或者请求方最新日志小于自身，则拒绝
	*/
	lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()
	if r.voteFor == 0 || r.voteFor == mCandidateId {
		if mTerm >= r.currentTerm && mLastLogIndex >= lastLogIndex {
			r.voteFor = mCandidateId
			success = true
		}
	}
	r.logger.Debugf("候选人: %s, 投票: %t ", strconv.FormatUint(mCandidateId, 16), success)
	r.send(&pb.RaftMessage{
		Type:         pb.MessageType_VOTE_RESP,
		Term:         mTerm,
		From:         r.id,
		To:           mCandidateId,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		Success:      success,
	})
	return
}

func (r *Raft) ReciveVoteResp(from, term, lastLogIndex, lastLogTerm uint64, success bool) {
	leadLastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	r.cluster.Vote(from, success)
	r.cluster.ResetLogIndex(from, lastLogIndex, leadLastLogIndex)

	voteRes := r.cluster.CheckVoteResult()
	if voteRes == VoteWon {
		r.logger.Debugf("节点 %s 发起投票，选举成功", strconv.FormatUint(r.id, 16))
		//重置没有给他投票的节点的日志索引
		for k, v := range r.cluster.voteResp {
			if !v {
				r.cluster.ResetLogIndex(k, lastLogIndex, leadLastLogIndex)
			}
		}
		r.SwitchLeader()
		r.BroadcastAppendEntries()
	} else if voteRes == VoteLost {
		r.logger.Debugf("节点 %s 发起投票，选举失败", strconv.FormatUint(r.id, 16))
		r.voteFor = 0
		r.cluster.ResetVoteResult()
	}
}

func (r *Raft) SwitchLeader() {
	r.logger.Debugf("leader,任期 %d", r.currentTerm)

	r.state = LeaderState
	r.leader = r.id
	r.voteFor = 0
	r.Tick = r.TickHeartbeat
	r.handleMessage = r.HandleLeaderMessage
	r.electtionTick = 0
	r.heartbeatTimeout = 0
	r.cluster.Reset()
}

func (r *Raft) HandleLeaderMessage(msg *pb.RaftMessage) {
	switch msg.Type {
	case pb.MessageType_PROPOSE:
		r.AppendEntry(msg.Entry)
	}
}

func (r *Raft) BroadcastAppendEntries() {

}

func (r *Raft) TickHeartbeat() {

}

// HandleMessage 消息处理
func (r *Raft) HandleMessage(msg *pb.RaftMessage) {
	if msg == nil {
		return
	}

	if msg.Term < r.currentTerm {
		r.logger.Debugf("节点 %s 收到旧任期消息:(%s)", strconv.FormatUint(r.id, 16), msg)
		return
	} else if msg.Term > r.currentTerm {
		if msg.Type != pb.MessageType_VOTE {
			// 日志追加，心跳，同步，节点成为对方follower
			if msg.Type == pb.MessageType_APPEND_ENTRY || msg.Type == pb.MessageType_HEARTBEAT || msg.Type == pb.MessageType_INSTALL_SNAPSHOT {
				r.SwitchFollower(msg.From, 0)
			}
		}
	}
	r.handleMessage(msg)
}

// SwitchFollower 转为follower
func (r *Raft) SwitchFollower(leaderId, term uint64) {
	r.state = FollowerState
	r.leader = leaderId
	r.currentTerm = term
	r.voteFor = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Tick = r.TickElection
	r.handleMessage = r.HandleFollowerMessage
	r.electtionTick = 0
	r.cluster.Reset()

	r.logger.Debugf("转为follower,leader %s, 任期 %d,选取周期 %d", strconv.FormatUint(leaderId, 16), term, r.randomElectionTimeout)
}

func (r *Raft) HandleFollowerMessage(msg *pb.RaftMessage) {
	switch msg.Type {
	case pb.MessageType_VOTE:
		grant := r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
		if grant {
			r.electtionTick = 0
		}
	case pb.MessageType_HEARTBEAT:
		r.electionTimeout = 0
		r.ReciveHeartbeat(msg.From, msg.Term, msg.LastLogIndex, msg.LastCommit, msg.Context)
	case pb.MessageType_APPEND_ENTRY:
		r.electionTimeout = 0
		r.ReciveAppendEntries(msg.From, msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit, msg.Entry)
	default:
		r.logger.Debugf("收到 %s 异常消息 %s 任期 %d", strconv.FormatUint(msg.From, 16), msg.Type, msg.Term)
	}
}

func (r *Raft) HandleCandiateMessage(msg *pb.RaftMessage) {
	switch msg.Type {
	case pb.MessageType_VOTE:

		grant := r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
		if grant {
			r.electtionTick = 0
		}
	case pb.MessageType_VOTE_RESP:
		r.ReciveVoteResp(msg.From, msg.Term, msg.LastLogIndex, msg.LastLogTerm, msg.Success)

	case pb.MessageType_HEARTBEAT:
		r.SwitchFollower(msg.From, msg.Term)
		r.ReciveHeartbeat(msg.From, msg.Term, msg.LastLogIndex, msg.LastCommit, msg.Context)

	case pb.MessageType_APPEND_ENTRY:
		r.SwitchFollower(msg.From, msg.Term)
		r.ReciveAppendEntries(msg.From, msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit, msg.Entry)
	default:
		r.logger.Debugf("收到 %s 异常消息 %s 任期 %d", strconv.FormatUint(msg.From, 16), msg.Type, msg.Term)
	}
}
func (r *Raft) ReciveHeartbeat(mFrom, mTerm, mLastLogIndex, mLastCommit uint64, context []byte) {
	lastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	r.raftlog.Apply(mLastCommit, lastLogIndex)

	r.send(&pb.RaftMessage{
		Type:    pb.MessageType_HEARTBEAT_RESP,
		Term:    r.currentTerm,
		From:    r.id,
		To:      mFrom,
		Context: context,
	})
}

func (r *Raft) ReciveAppendEntries(mLeader, mTerm, mLastLogTerm, mLastLogIndex, mLastCommit uint64, mEntries []*pb.LogEntry) {
	//todo
}

func (r *Raft) AppendEntry(entry []*pb.LogEntry) {

}
