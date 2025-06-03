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
	cluster               *Cluster              // 集群节点
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
