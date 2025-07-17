package skiplist

import (
	"bytes"
	"math/rand"
	"sync"
)

// 跳表最大高度
const tMaxHeight = 12
const (
	nKV     = iota
	nKey    // key偏移
	nVal    //value偏移
	nHeight //高度偏移
	nNext   //下一跳位置偏移
)

type SkipList struct {
	mu        sync.RWMutex // 读写锁
	rand      *rand.Rand   //随机函数，判断数据是否存在于某层
	kvData    []byte
	kvNode    []int           //实际数据 0偏移 1key长度 2value长度 3高度 4+h-1各高度下一跳位置
	maxHeight int             //最大高度
	prevNode  [tMaxHeight]int //上一个节点，用于回溯
	kvSize    int             //数据大小
}

type SkipListIter struct {
	sl         *SkipList
	node       int    //当前节点
	Key, Value []byte //当前节点的key value
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
func (s *SkipList) Put(key, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	lenv := len(value)
	lenk := len(key)

	//已经存在直接更新
	n, b := s.getNode(key)
	keystart := len(s.kvData)
	s.kvData = append(s.kvData, key...)
	s.kvData = append(s.kvData, value...)
	if b {
		s.kvSize += lenv - s.kvNode[n+nVal]
		s.kvNode[n] = keystart
		s.kvNode[n+nVal] = lenv
		return
	}
	h := s.randHeight()
	if h > s.maxHeight {
		s.maxHeight = h
		for i := s.maxHeight; i < h; i++ {
			s.prevNode[i] = 0
		}
	}
	n = len(s.kvNode)

	s.kvNode = append(s.kvNode, keystart, lenk, lenv, h)
	for i, node := range s.prevNode[:h] {
		m := node + nNext + i
		s.kvNode = append(s.kvNode, s.kvNode[m])
		s.kvNode[m] = n
	}
	s.kvSize += lenv + lenk
}

func (s *SkipList) randHeight() int {
	const branching = 4
	h := 1
	for h < tMaxHeight && s.rand.Int()%branching == 0 {
		h++
	}
	return h
}

// Next 获取下一跳数据
func (i *SkipListIter) Next() bool {
	i.node = i.sl.kvNode[i.node+nNext]
	if i.node == 0 {
		return false
	}
	keyStart := i.sl.kvNode[i.node]
	keyEnd := keyStart + i.sl.kvNode[i.node+nKey]
	valueEnd := keyStart + i.sl.kvNode[i.node+nVal]

	i.Key = i.sl.kvData[keyStart:keyEnd]
	i.Value = i.sl.kvData[keyEnd:valueEnd]
	return true
}
func NewSkipListIter(sl *SkipList) *SkipListIter {
	return &SkipListIter{sl: sl}
}
func NewSkipList() *SkipList {
	s := &SkipList{
		rand:      rand.New(rand.NewSource(0xdeadbeef)),
		maxHeight: 1,
		kvData:    make([]byte, 0),
		kvNode:    make([]int, 4+tMaxHeight),
	}
	s.kvNode[nHeight] = tMaxHeight
	return s
}
