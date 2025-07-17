package raft

import (
	"MQ/pb"
	"MQ/skiplist"
	"MQ/wal"
	"go.uber.org/zap"
	"time"
)

const LOG_SNAPSHOT_SIZE = 32 * 1024 * 1024
const WAL_FLUSH_INTERVAL = 10 * time.Second

type Storage interface {
	Append(entries []*pb.LogEntry)
	GetEntries(startIndex, endIndex uint64) []*pb.LogEntry
	GetTerm(index uint64) uint64
	GetLastLogIndexAndTerm() (uint64, uint64)
	Close()
}

type RaftStorage struct {
	encoding            Encoding           // 日志编解码
	walw                *wal.WalWriter     // 预写日志
	logEntries          *skiplist.SkipList // raft 日志
	logState            *skiplist.SkipList // kv 数据
	immutableLogEntries *skiplist.SkipList // 上次/待写入快照 raft日志
	immutableLogState   *skiplist.SkipList // 上次/待写入快照 kv 数据
	snap                *Snapshot          // 快照实例
	stopc               chan struct{}      // 停止通道
	logger              *zap.SugaredLogger
}

//	func (rs *RaftStorage) Append(entries []*pb.LogEntry) {
//		for _, entry := range entries {
//			logKey, logValue := rs.encoding.EncodeLogEntry(entry)
//		}
//	}
func (rs *RaftStorage) checkFlush() {
	go func() {
		ticker := time.NewTicker(WAL_FLUSH_INTERVAL)
		for {
			select {
			case <-ticker.C:
				rs.walw.Flush()
			case <-rs.stopc:
				rs.walw.Flush()
				return
			}
		}
	}()
}
