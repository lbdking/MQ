package raft

import (
	"go.uber.org/zap"
)

type VoteResult int

const (
	Voting VoteResult = iota
	VoteWon
	VoteLost
)

type ReadIndexResp struct {
	Req   []byte
	Index uint64
	Send  uint64
	ack   map[uint64]bool
}

type Cluster struct {
	incoming           map[uint64]struct{}         //当前集群节点，，存储节点id
	outcoming          map[uint64]struct{}         //旧集群节点
	pendingChangeIndex uint64                      //记录未完成的变更日志
	inJoint            bool                        //是否正在进行联合共识
	voteResp           map[uint64]bool             //存储投票响应结果
	pendingReadIndex   map[string]*ReadIndexResp   //处理线性化读请求的状态，键为请求 ID，值为 ReadIndexResp 结构体，包含请求内容、索引和应答状态。
	progress           map[uint64]*ReplicaProgress //各节点日志复制进度
	logger             *zap.SugaredLogger
}

func (c *Cluster) AddReadIndex(from, index uint64, req []byte) {
	c.pendingReadIndex[string(req)] = &ReadIndexResp{
		Req:   req,
		Index: index,
		Send:  from,
		ack:   map[uint64]bool{},
	}
}

func (c *Cluster) Vote(id uint64, granted bool) {
	c.voteResp[id] = granted
}

func (c *Cluster) ResetVoteResult() {
	c.voteResp = make(map[uint64]bool)
}

func (c *Cluster) Foreach(f func(id uint64, p *ReplicaProgress)) {
	for id, p := range c.progress {
		f(id, p)
	}
}

func (c *Cluster) ResetLogIndex(from, lastLogIndex, leadLastLogIndex uint64) {

}

func (c *Cluster) CheckVoteResult() VoteResult {
	granted, lost := 0, 0
	for _, v := range c.voteResp {
		if v {
			granted++
		} else {
			lost++
		}
	}
	if granted >= len(c.incoming)/2+1 {
		return VoteWon
	} else if lost > len(c.incoming)/2 {
		return VoteLost
	} else {
		return Voting
	}
}

func (c *Cluster) Reset() {

}
