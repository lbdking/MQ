package lsm

import (
	"MQ/filter"
	"bytes"
	"encoding/binary"
	"fmt"
	"go.uber.org/zap"
	"os"
	"path"
)

type SstWriter struct {
	conf            *Config
	fd              *os.File            // sst文件
	dataBuf         *bytes.Buffer       //数据缓冲
	filterBuf       *bytes.Buffer       //过滤器缓冲
	IndexBuf        *bytes.Buffer       //索引缓冲
	index           []*Index            // 索引
	filter          map[uint64][]byte   // 过滤器
	bf              *filter.BloomFilter // 过滤器生成
	dataBlock       *Block              // 数据块
	filterBlock     *Block              // 过滤器块
	indexBlock      *Block              // 索引块
	indexScratch    [20]byte            // 辅助byte数组，将uint64转换成变长[]byte
	prevKey         []byte              //前一个key,生成分割数据块的索引key
	prevBlockOffSet uint64              // 前一个数据块的偏移，生成分隔索引
	prevBlockSize   uint64              // 前一个数据块的大小
	logger          *zap.SugaredLogger
}

func (w *SstWriter) Append(key, value []byte) {
	//	如果数据量为0,添加分隔索引
	if w.dataBlock.nEntries == 0 {
		skey := make([]byte, len(key))
		copy(skey, key)
		w.addIndex(skey)
	}
	//添加数据
	w.dataBlock.Append(key, value)
	//添加到过滤器
	w.bf.Add(key)
	//记录前一个key
	w.prevKey = key

	//数据块大小超过阈值，则刷盘
	if w.dataBlock.Size() > w.conf.SstDataBlockSize {
		w.flushBlock()
	}
}
func (w *SstWriter) addIndex(key []byte) {
	//记录前一个数据块偏移量以及大小编码到缓冲区中
	n := binary.PutUvarint(w.indexScratch[0:], w.prevBlockOffSet)
	n += binary.PutUvarint(w.indexScratch[n:], w.prevBlockSize)
	//计算前一个key和当前key的分隔符（公共前缀）
	separator := GetSeparator(w.prevKey, key)
	//将分隔符和数据块偏移量编码到索引块中
	w.indexBlock.Append(separator, w.indexScratch[:n])
	//保存索引到数组中
	w.index = append(w.index, &Index{
		Key:    separator,
		Offset: w.prevBlockOffSet,
		Size:   w.prevBlockSize,
	})
}

func (w *SstWriter) flushBlock() {
	var err error
	//记录当前数据缓冲大小，在下次添加分隔索引时使用
	w.prevBlockOffSet = uint64(w.dataBuf.Len())
	n := binary.PutUvarint(w.indexScratch[0:], uint64(w.prevBlockOffSet))
	//生成布隆过滤器hash,记录到map中
	filter := w.bf.Hash()
	w.filter[w.prevBlockOffSet] = filter
	//添加数据块偏移到布隆过滤器关系到过滤块中
	w.filterBlock.Append(w.indexScratch[:n], filter)
	//重置布隆过滤器
	w.bf.Reset()

	//将当前数据块写入数据缓冲之中
	w.prevBlockSize, err = w.dataBlock.FlushBlockTo(w.dataBuf)
	if err != nil {
		w.logger.Errorln("写入数据块失败", err)
	}
}
func GetSeparator(a, b []byte) []byte {
	//首个数据块，没有公共前缀，
	if len(a) == 0 {
		n := len(b) - 1
		c := b[n] - 1
		return append(b[0:n], c)
	}
	n := SharedPrefixLen(a, b)
	/*
		无公共前缀或者公共前缀为数据块的key，返回a
		返回公共前缀为数据块的key
	*/
	if n == 0 || n == len(a) {
		return a
	} else {
		c := a[n] + 1
		return append(a[0:n], c)
	}
}

func (w *SstWriter) Finish() (int64, map[uint64][]byte, []*Index) {
	if w.bf.KeyLen() > 0 {
		w.flushBlock()
	}
	//将过滤块写入到过滤缓冲
	if _, err := w.filterBlock.FlushBlockTo(w.filterBuf); err != nil {
		w.logger.Errorln("写入过滤块失败", err)
	}
	//添加分隔索引，将索引写入到索引缓冲中
	w.addIndex(w.prevKey)
	if _, err := w.indexBlock.FlushBlockTo(w.IndexBuf); err != nil {
		w.logger.Errorln("写入索引块失败", err)
	}
	//生成sst文件，记录各部分偏移，大小
	footer := make([]byte, w.conf.SstFooterSize)
	size := w.dataBuf.Len()
	//metdata索引起始偏移，大小
	n := binary.PutUvarint(footer[0:], uint64(size))
	n += binary.PutUvarint(footer[n:], uint64(w.filterBuf.Len()))
	size += w.filterBuf.Len()
	n += binary.PutUvarint(footer[n:], uint64(size))
	n += binary.PutUvarint(footer[n:], uint64(w.IndexBuf.Len()))
	size += w.IndexBuf.Len()
	size += w.conf.SstFooterSize

	//将缓冲写入sst文件
	w.fd.Write(w.dataBuf.Bytes())
	w.fd.Write(w.filterBuf.Bytes())
	w.fd.Write(w.IndexBuf.Bytes())

	return int64(size), w.filter, w.index
}

func NewSsWriter(file string, conf *Config, logger *zap.SugaredLogger) (*SstWriter, error) {
	fd, err := os.OpenFile(path.Join(conf.Dir, file), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("创建 %s 失败", file, err)
	}
	return &SstWriter{
		conf:        conf,
		fd:          fd,
		dataBuf:     bytes.NewBuffer(make([]byte, 0)),
		filterBuf:   bytes.NewBuffer(make([]byte, 0)),
		IndexBuf:    bytes.NewBuffer(make([]byte, 0)),
		filter:      make(map[uint64][]byte),
		index:       make([]*Index, 0),
		bf:          filter.NewBloomFilter(10),
		dataBlock:   NewBlock(conf),
		filterBlock: NewBlock(conf),
		indexBlock:  NewBlock(conf),
		prevKey:     make([]byte, 0),
		logger:      logger,
	}, nil
}
