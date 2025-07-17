package lsm

import (
	"bytes"
	"go.uber.org/zap"
	"sync"
)

type Node struct {
	wg         sync.WaitGroup    //等待外部读取文件完成
	sr         *SstReader        // sst文件读取
	filter     map[uint64][]byte //布隆过滤器
	startKey   []byte            //起始key
	endKey     []byte            //结束key
	index      []*Index          //索引数组
	Level      int               //lsm层数
	SeqNo      int               //lsm节点序号
	Extra      string            //额外信息
	FileSize   int64             //文件大小
	compacting bool              //是否正在compact
	// 遍历使用
	curBlock int           //当前读取块
	curBuf   *bytes.Buffer //当前读取到缓存
	prevKey  []byte        //上一个key
	logger   *zap.SugaredLogger
}

func (n *Node) Close() {
	n.sr.Close()
}
