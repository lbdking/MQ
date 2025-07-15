package raft

import (
	"fmt"
	"math"
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
	nums := []int{4, 5, 6, 7, 0, 1, 2}
	fmt.Println(search(nums, 0))
}

func search(nums []int, target int) int {
	if nums[0] == target {
		return 0
	}
	left := 0
	right := len(nums) - 1
	if right == 1 {
		if nums[1] == target {
			return 1
		}
		return -1
	}
	mid := (right + left) / 2
	index := -1
	for left < right {
		if nums[mid] == target {
			index = mid
			return index
		}
		if nums[mid] < nums[right] && nums[mid] > target {
			right = mid - 1
		}
		if nums[mid] < nums[right] && nums[mid] < target {
			left = mid + 1
		}
		if nums[mid] > nums[left] && nums[left] < target {
			right = mid - 1
		}
		if nums[mid] > nums[left] && nums[mid] < target {
			left = mid + 1
		}
		mid = (right + left) / 2
	}
	return index
}
func maxAdjacentDistance(nums []int) int {
	max := int(math.Abs(float64(nums[len(nums)-1]))) - nums[0]
	for i := 0; i < len(nums)-1; i++ {
		max = int(math.Max(float64(max), float64(int(math.Abs(float64(nums[i]-nums[i+1]))))))
	}
	return max
}
