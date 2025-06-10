package raft

import (
	"fmt"
	"sort"
	"testing"
)

func TestApply(t *testing.T) {
	l := NewRaftLog(nil, nil)
	l.TestApply(1, 1)
}

func (l *RaftLog) TestApply(lastCommit uint64, lastLogIndex uint64) {
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
		entries := l.logEntries[:n+1]

		l.storage.Append(entries)
		l.lastAppliedIndex = l.logEntries[n].Index
		l.lastAppliedTerm = l.logEntries[n].Term
		l.logEntries = l.logEntries[n+1:]

		l.NotifyReadIndex()
	}
}

func TestSort(t *testing.T) {
	nums := []int{5, 2, 3, 1, 4}
	sort.Ints(nums)
	fmt.Print(nums)
}
