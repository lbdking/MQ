package raft

import (
	"MQ/pb"
	"go.uber.org/zap"
)

type Storage interface {
	Append(entries []*pb.LogEntry)
	GetEntries(startIndex, endIndex uint64) []*pb.LogEntry
	GetTerm(index uint64) uint64
	GetLastLogIndexAndTerm() (uint64, uint64)
	Close()
}

type RaftStorage struct {
	stopc  chan struct{} // 停止通道
	logger *zap.SugaredLogger
}
