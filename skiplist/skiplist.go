package skiplist

import (
	"bytes"
	"math/rand"
	"sync"
)

// 跳表最大高度
const tMaxHeight = 12
const (
	nKV = iota
	nKey
	nVal
	nHeight
	nNext
)

type SkipList struct {
	mu        sync.RWMutex // 读写锁
	rand      *rand.Rand   //随机函数，判断数据是否存在于某层
	kvData    []byte       //实际数据 0偏移 1key长度 2value长度 3高度 4+h-1各高度下一跳位置
	kvNode    []int
	maxHeight int             //最大高度
	prevNode  [tMaxHeight]int //上一个节点，用于回溯
	kvSize    int             //数据大小
}

func (s *SkipList) getNode(key []byte) (int, bool) {
	n := 0
	h := s.maxHeight - 1
	for {
		next := s.kvNode[n+nNext+h]
		cmp := 1
		if next != 0 {
			keyStart := s.kvNode[next]
			keyLen := s.kvNode[next+nKey]
			cmp = bytes.Compare(s.kvData[keyStart:keyStart+keyLen], key)
		}
		if cmp == 0 {
			return next, true
		} else if cmp > 0 {
			n = next
		} else {
			s.prevNode[h] = n
			if h > 0 {
				h--
			} else {
				return next, false
			}
		}
	}
}
func (s *SkipList) Get(key []byte) []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, b := s.getNode(key)
	if b {
		keyStart := s.kvNode[n]
		keyLen := s.kvNode[n+nKey]
		valueLne := s.kvNode[n+nVal]
		return s.kvData[keyStart+keyLen : keyStart+keyLen+valueLne]
	} else {
		return nil
	}
}
