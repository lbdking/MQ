package raft

import (
	"MQ/pb"
	"go.uber.org/zap"
)

const MAX_APPEND_ENTRT_SIZE = 1000

type RaftLog struct {
	logEntries       []*pb.LogEntry //未提交日志
	storage          Storage        //已提交日志存储
	commitIndex      uint64         //已提交日志索引（进度）
	lastAppliedIndex uint64         //最后应用日志索引
	lastAppliedTerm  uint64         //最后提交日志任期
	lastAppendIndex  uint64         //最后追加日志索引
	waitQueue        []*WaitApply   //等待提交通知
	logger           *zap.SugaredLogger
}

type WaitApply struct {
	done  bool
	index uint64
	ch    chan struct{}
}

// NewRaftLog todo
func NewRaftLog(storage Storage, logger *zap.SugaredLogger) *RaftLog {
	lastIndex, lastTerm := storage.GetLastLogIndexAndTerm()

	return &RaftLog{
		logEntries:       make([]*pb.LogEntry, 0),
		storage:          storage,
		commitIndex:      lastIndex,
		lastAppliedIndex: lastIndex,
		lastAppliedTerm:  lastTerm,
		logger:           logger,
	}
}

func (l *RaftLog) GetLastLogIndexAndTerm() (lastLogIndex, lastLogTerm uint64) {
	if len(l.logEntries) > 0 {
		lastLog := l.logEntries[len(l.logEntries)-1]
		lastLogIndex = lastLog.Index
		lastLogTerm = lastLog.Term
	} else {
		lastLogIndex = l.lastAppliedIndex
		lastLogTerm = l.lastAppliedTerm
	}
	return
}

// Apply 日志提交
func (l *RaftLog) Apply(lastCommit uint64, lastLogIndex uint64) {
	//  更新可提交日志索引
	if lastCommit > l.commitIndex {
		if lastCommit > l.commitIndex {
			l.commitIndex = lastCommit
		} else {
			l.commitIndex = lastLogIndex
		}
	}
	//提交索引
	if l.commitIndex > l.lastAppliedIndex {
		n := 0
		for i, entry := range l.logEntries {
			if l.commitIndex >= entry.Index {
				n = i
			} else {
				break
			}
		}
		if n == 0 {
			return
		}
		entries := l.logEntries[:n+1]

		l.storage.Append(entries)
		l.lastAppliedIndex = l.logEntries[n].Index
		l.lastAppliedTerm = l.logEntries[n].Term
		l.logEntries = l.logEntries[n+1:]

		l.NotifyReadIndex()
	}
}

// Append 追加日志
func (l *RaftLog) Append(entries []*pb.LogEntry) {
	size := len(entries)
	if size == 0 {
		return
	}
	l.logEntries = append(l.logEntries, entries...)
	l.lastAppendIndex = entries[size-1].Index
}
func (l *RaftLog) HasPrevLog(lastIndex, lastTerm uint64) bool {
	if lastIndex == 0 {
		return true
	}
	var term uint64
	//获取未提交日志长度
	size := len(l.logEntries)
	if size > 0 {
		//获取最后一条日志
		lastLog := l.logEntries[size-1]
		/*
		 * 1. lastIndex == lastLog.Index 最后一条日志索引等于lastIndex，记录term
		 * 2. lastIndex > lastLog.Index
		 * 3. lastIndex < lastLog.Index
		 */
		if lastLog.Index == lastIndex {
			term = lastLog.Term
		} else if lastLog.Index > lastIndex {
			if lastIndex == l.lastAppendIndex {
				l.logEntries = l.logEntries[:0]
				return true
			} else if lastLog.Index > l.lastAppendIndex {
				for i, entry := range l.logEntries[:size] {
					if entry.Index == lastIndex {
						term = entry.Term
						l.logEntries = l.logEntries[:i+1]
						break
					}
				}
			} else if lastLog.Index == lastIndex {
			}
		}
	} else if lastIndex == l.lastAppliedIndex {
		return true
	}
	b := term == lastTerm
	if !b {
		l.logger.Debugf("最新日志: %d ,任期: %d , 本地记录任期: %d", lastIndex, lastTerm, term)
		if term != 0 {
			for i, entry := range l.logEntries {
				if entry.Term == term {
					l.logEntries = l.logEntries[:i]
					break
				}
			}
		}
	}
	return b
}

// AppendEntry 日志追加
// -将日志追加到内存切片，更新lastAppendIndex
func (l *RaftLog) AppendEntry(entry []*pb.LogEntry) {
	size := len(entry)
	if size == 0 {
		return
	}
	l.logEntries = append(l.logEntries, entry...)
	l.lastAppendIndex = entry[size-1].Index
}

// NotifyReadIndex
// todo
func (l *RaftLog) NotifyReadIndex() {

}

// todo
func (l *RaftLog) GetTerm(lastLogIndex uint64) uint64 {
	return 0
}

// GetEntries 从leader节点中获取日志
func (l *RaftLog) GetEntries(index uint64, maxSize int) []*pb.LogEntry {
	if index <= l.lastAppliedIndex {
		endIndex := index + MAX_APPEND_ENTRT_SIZE
		if endIndex >= l.lastAppliedIndex {
			endIndex = l.lastAppliedIndex + 1
		}
		return l.storage.GetEntries(index, endIndex)
	} else {
		var entries []*pb.LogEntry
		for i, entry := range l.logEntries {
			if entry.Index == index {
				if len(l.logEntries)-i > maxSize {
					entries = l.logEntries[i : i+maxSize]
				} else {
					entries = l.logEntries[i:]
				}
				break
			}
		}
		return entries
	}
}

// GetSnapshot
// todo
func (l *RaftLog) GetSnapshot(index uint64) (interface{}, interface{}) {
	return nil, nil
}
