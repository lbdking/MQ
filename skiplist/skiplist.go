package skiplist

import (
	"math/rand"
	"sync"
)

// 跳表最大高度
const tMaxHeight = 12

type SkipList struct {
	mu        sync.RWMutex
	rand      *rand.Rand
	kvData    []byte
	kvNode    []int
	maxHeight int
	prevNode  [tMaxHeight]int
	kvSize    int
}
