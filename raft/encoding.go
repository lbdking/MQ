package raft

import (
	"MQ/pb"
	"MQ/skiplist"
)

type Encoding interface {
	// 编码日志索引
	EncodeIndex(index uint64) []byte
	// 解码日志索引
	DecodeIndex(key []byte) uint64
	// 编码日志条目
	EncodeLogEntry(entry *pb.LogEntry) ([]byte, []byte)
	// 解码日志条目
	DecodeLogEntry(key, value []byte) *pb.LogEntry
	// 批量解码日志条目(raft log -> kv  )
	DecodeLogEntries(logEntry *skiplist.SkipList) (*skiplist.SkipList, uint64, uint64)
	// 编码日志条目键值对
	EncodeLogEntryData(key, value []byte) []byte
	// 解码日志条目键值对
	DecodeLogEntryData(entry []byte) ([]byte, []byte)
	// 添加默认前缀
	DefaultPrefix(key []byte) []byte
	// 添加集群成员前缀
	MemberPrefix(key []byte) []byte
}
