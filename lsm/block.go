package lsm

import (
	"MQ/utils"
	"bytes"
	"encoding/binary"
	"io"

	"github.com/golang/snappy"
)

type Block struct {
	conf               *Config
	header             [30]byte      //存储元数据（前缀长度，键值对长度）
	record             *bytes.Buffer //存储实际数据
	trailer            *bytes.Buffer //存储重启点偏移量
	nEntries           int
	prevKey            []byte //记录上一个key,用于计算共享前缀
	compressionScratch []byte
}

func (b *Block) Append(key, value []byte) {
	keyLen := len(key)
	valueLen := len(value)
	nSharePrefix := 0
	//重启点，间隔一定数据之后，重新开始键共享
	if b.nEntries%b.conf.SsRestartInterval == 0 {
		//重启点用4字节记录偏移量
		buf4 := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf4, uint32(b.record.Len()))
		b.trailer.Write(buf4)
	} else {
		nSharePrefix = SharedPrefixLen(b.prevKey, key)
	}

	//编码共享前缀长度
	n := binary.PutUvarint(b.header[0:], uint64(nSharePrefix))
	//编码非共享键长度
	n += binary.PutUvarint(b.header[n:], uint64(keyLen-nSharePrefix))
	//编码值长度
	n += binary.PutUvarint(b.header[n:], uint64(valueLen))

	//data
	b.record.Write(b.header[:n])
	b.record.Write(key[nSharePrefix:])
	b.record.Write(value)

	b.prevKey = append(b.prevKey[:0], key...)
	b.nEntries++
}

// SharedPrefixLen 计算相邻键值对相同前缀
func SharedPrefixLen(a, b []byte) int {
	i, n := 0, len(a)
	if n > len(b) {
		n = len(b)
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

func (b *Block) compress() []byte {

	//最后4字节记录重启节点数量
	buf4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf4, uint32(b.trailer.Len()/4))
	b.trailer.Write(buf4)

	//将重启点写入记录缓冲
	b.record.Write(b.trailer.Bytes())
	//计算并分配压缩需要空间
	n := snappy.MaxEncodedLen(b.record.Len())

	if n > len(b.compressionScratch) {
		b.compressionScratch = make([]byte, n+b.conf.SstBlockTrailerSize)
	}

	//压缩记录
	compressed := snappy.Encode(b.compressionScratch, b.record.Bytes())

	//添加crc校验到块尾
	crc := utils.Checksum(compressed)
	size := len(compressed)
	compressed = compressed[:size+b.conf.SstBlockTrailerSize]
	binary.LittleEndian.PutUint32(compressed[size:], crc)

	return compressed
}

// 清空块缓冲
func (b *Block) clear() {
	b.nEntries = 0
	b.prevKey = b.prevKey[:0]
	b.record.Reset()
	b.trailer.Reset()
}

// 将块写入到指定的writer中，写入后清空块缓冲

func (b *Block) FlushBlockTo(dest io.Writer) (uint64, error) {
	defer b.clear()

	n, err := dest.Write(b.compress())
	return uint64(n), err
}

func (b *Block) Size() int {
	return b.record.Len() + b.trailer.Len() + 4
}

func NewBlock(conf *Config) *Block {
	return &Block{
		record:  bytes.NewBuffer(make([]byte, 0)),
		trailer: bytes.NewBuffer(make([]byte, 0)),
		conf:    conf,
	}
}
