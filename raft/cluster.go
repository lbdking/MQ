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

func NewCluster(peers map[uint64]string, lastIndex uint64, logger *zap.SugaredLogger) *Cluster {

	incoming := make(map[uint64]struct{})
	progress := make(map[uint64]*ReplicaProgress)
	for id := range peers {
		progress[id] = &ReplicaProgress{
			NextIndex:  lastIndex + 1,
			MatchIndex: lastIndex,
		}
		incoming[id] = struct{}{}
	}
	return &Cluster{
		incoming:         incoming,
		outcoming:        make(map[uint64]struct{}),
		voteResp:         make(map[uint64]bool),
		progress:         progress,
		pendingReadIndex: make(map[string]*ReadIndexResp),
		logger:           logger,
	}
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

// GetNextIndex
// todo
func (c *Cluster) GetNextIndex(id uint64) uint64 {
	p := c.progress[id]
	if p != nil {
		return p.NextIndex
	}
	return 0
}

// AppendEntry
// todo
func (c *Cluster) AppendEntry(id uint64, lastIndex uint64) {
	p := c.progress[id]
	if p != nil {
		p.AppendEntry(lastIndex)
	}
}

// UpdateLogIndex
// todo
func (c *Cluster) UpdateLogIndex(id uint64, index uint64) {

}

// CheckCommit 检查集群是否可以提交日志
func (c *Cluster) CheckCommit(index uint64) bool {
	incomingLogged := 0
	for id := range c.progress {
		if index <= c.progress[id].MatchIndex {
			incomingLogged++
		}
	}
	incomingCommit := incomingLogged > len(c.incoming)/2+1
	return incomingCommit
}

func (c *Cluster) AppendEntryResp(id, lastIndex uint64) {
	p := c.progress[id]
	if p != nil {
		p.AppendEntry(lastIndex)
	}
}
func (c *Cluster) ResetLogIndex(id, lastIndex, leaderLastIndex uint64) {
	p := c.progress[id]
	if p != nil {
		p.ResetLogIndex(lastIndex, leaderLastIndex)
	}
}
