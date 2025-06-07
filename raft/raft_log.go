package raft

import (
	"MQ/pb"
	"go.uber.org/zap"
)

type RaftLog struct {
	logEnties        []*pb.LogEntry //未提交日志
	storage          Storage        //已提交日志存储
	commitIndex      uint64         //已提交日志索引（进度）
	lastAppliedIndex uint64         //最后应用日志索引
	lastAppliedTerm  uint64         //最后提交日志任期
	lastAppendIndex  uint64         //最后追加日志索引
	logger           *zap.SugaredLogger
}

type Storage interface {
	Append(entries []*pb.LogEntry)
	GetEntries(startIndex, endIndex uint64) []*pb.LogEntry
	GetTerm(index uint64) uint64
	GetLastLogIndexAndTerm() (uint64, uint64)
	Close()
}

// todo
func NewRaftLog(logger *zap.SugaredLogger) *RaftLog {
	return nil
}

func (l *RaftLog) GetLastLogIndexAndTerm() (lastLogIndex, lastLogTerm uint64) {
	if len(l.logEnties) > 0 {
		lastLog := l.logEnties[len(l.logEnties)-1]
		lastLogIndex = lastLog.Index
		lastLogTerm = lastLog.Term
	} else {
		lastLogIndex = l.lastAppliedIndex
		lastLogTerm = l.lastAppliedTerm
	}
	return
}

// todo
func (l *RaftLog) Apply(commit uint64, index uint64) {

}
