package raft

import (
	"MQ/pb"
)

type ReplicaProgress struct {
	MatchIndex         uint64            // 已接收日志
	NextIndex          uint64            // 下次发送日志
	pending            []uint64          // 未发送完成日志
	prevResp           bool              // 上次日志发送结果
	maybeLostIndex     uint64            // 可能丢失的日志,记上次发送未完以重发
	installingSnapshot bool              // 是否发送快照中
	snapc              chan *pb.Snapshot // 快照读取通道
	prevSnap           *pb.Snapshot      // 上次发送快照
	maybePrevSnapLost  *pb.Snapshot      // 可能丢失快照,标记上次发送未完成以重发
}

func (rp *ReplicaProgress) MaybeLogLost(u uint64) bool {
	return (!rp.prevResp && len(rp.pending) > 0)
}

// IsPause
// todo
func (rp *ReplicaProgress) IsPause() bool {
	return false
}

// AppendEntry
// todo
func (rp *ReplicaProgress) AppendEntry(lastIndex uint64) {
	rp.pending = append(rp.pending, lastIndex)
	if rp.prevResp {
		rp.NextIndex = lastIndex + 1
	}
}

// AppendEntryResp
func (rp *ReplicaProgress) AppendEntryResp(lastIndex uint64) {
	if rp.MatchIndex < lastIndex {
		rp.MatchIndex = lastIndex
	}
	idx := -1
	for i, v := range rp.pending {
		if v == lastIndex {
			idx = i
		}
	}
	if !rp.prevResp {
		rp.prevResp = true
		rp.NextIndex = lastIndex + 1
	}
	if idx > -1 {
		rp.pending = rp.pending[idx+1:]
	}
}

// ResetLogIndex
func (rp *ReplicaProgress) ResetLogIndex(lastLogIndex uint64, leaderLastLogIndex uint64) {
	//节点最后的日志索引小于leader最新日志按节点更新进度，否则按leader更新进度
	if lastLogIndex < leaderLastLogIndex {
		rp.NextIndex = lastLogIndex + 1
		rp.MatchIndex = lastLogIndex
	} else {
		rp.NextIndex = leaderLastLogIndex + 1
		rp.MatchIndex = leaderLastLogIndex
	}

	if rp.prevResp {
		rp.prevResp = false
		rp.pending = nil
	}
}
