package lsm

import (
	"MQ/skiplist"
	"fmt"
	"go.uber.org/zap"
	"math"
	"sync"
	"time"
)

type Config struct {
	Dir                 string
	Logger              *zap.SugaredLogger
	MaxLevel            int //最大层数
	SstSize             int //sst文件大小
	SstDataBlockSize    int //sst数据块大小
	SstFooterSize       int //sst尾大小
	SstBlockTrailerSize int //Sst块尾大小
	SsRestartInterval   int //sst重启点间隔
}
type Tree struct {
	mu      sync.RWMutex
	conf    *Config
	tree    [][]*Node     //lsm
	seqNo   []int         //各层sst文件最新序号
	compacc chan int      //合并通知
	stopc   chan struct{} //停止通知
	logger  *zap.SugaredLogger
}

func NewTree(conf *Config) *Tree {
	compactionChan := make(chan int, 100)
	levelTree := make([][]*Node, conf.MaxLevel)

	for i := range levelTree {
		levelTree[i] = make([]*Node, 0)
	}
	lsmTree := &Tree{
		conf:    conf,
		tree:    levelTree,
		seqNo:   make([]int, conf.MaxLevel),
		compacc: compactionChan,
		stopc:   make(chan struct{}),
		logger:  conf.Logger,
	}
	lsmTree.CheckCompaction()
	return lsmTree
}
func (t *Tree) Close() {
	for {
		select {
		case t.stopc <- struct{}{}:
		case <-time.After(time.Second):
			close(t.stopc)
			close(t.compacc)
			for _, level := range t.tree {
				for _, node := range level {
					node.Close()
				}
			}
			return
		}
	}
}

func (t *Tree) FlushRecord(sl *skiplist.SkipList, extra string) error {
	level := 0
	seqNo := t.NextSeqNo(level)

	file := formatName(level, seqNo, extra)
	w,err := 
}

func formatName(level int, seqNo int, extra string) interface{} {
	return fmt.Sprintf("%d_%d_%s", level, seqNo, extra)
}

func (t *Tree) NextSeqNo(level int) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.seqNo[level]++
	return t.seqNo[level]
}

// CheckCompaction 检查合并节点
func (t *Tree) CheckCompaction() {
	level0 := make(chan struct{}, 100)
	levelN := make(chan int, 100)

	go func() {
		for {
			select {
			case <-level0:
				if len(t.tree[0]) > 4 {
					t.logger.Infof("Level0 开始合并，当前数量为 %d", len(t.tree[0]))
					t.compaction(0)
				}
			case <-t.stopc:
				close(level0)
				return
			}
		}
	}()
	go func() {
		for {
			select {
			case lv := <-levelN:
				var prevSize int64
				maxNodeSize := int64(t.conf.SstSize * int(math.Pow10(lv+1)))
				for {
					var totalSize int64
					for _,node := range t.tree[lv] {
						totalSize += node.FileSize
					}
					if totalSize > maxNodeSize && (prevSize == 0 || totalSize < prevSize) {
						t.logger.Infof("Level %d 当前大小: %d M, 最大大小: %d M, 执行合并", lv, totalSize/(1024*1024), maxNodeSize/(1024*1024))
						t.compaction(lv)
						prevSize = totalSize
					} else {
						break
					}
				}
			case <-t.stopc:
				close(levelN)
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case<-t.stopc:
				return
			case lv := <- t.compacc:
				if lv == 0 {
					level0 <- struct{}{}
				} else {
					levelN <- lv
				}
			}
		}
	}()
}

// todo
func (t *Tree) compaction(i int) {

}
