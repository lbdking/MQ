package lsm

import (
	"MQ/utils"
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/golang/snappy"
	"io"
	"os"
	"path"
	"sync"
)

// Index 索引格式
type Index struct {
	Key    []byte // 分隔键
	Offset uint64 // 前一数据块偏移
	Size   uint64 // 前一数据块大小
}

type SstReader struct {
	mu              sync.RWMutex
	conf            *Config
	fd              *os.File      // sst文件(读)
	reader          *bufio.Reader //包装file reader
	FilterOffset    int64         // 过滤块起始偏移
	FilterSize      int64         // 过滤块大小
	IndexOffset     int64         // 索引块起始偏移
	IndexSize       int64         // 索引块大小
	compressScratch []byte        // 解压缓冲
}

func NewSstReader(file string, conf *Config) (*SstReader, error) {
	fd, err := os.OpenFile(path.Join(conf.Dir, file), os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("无法加入节点，打开 %s文件失败:%v", file, err)
	}

	return &SstReader{
		fd:     fd,
		conf:   conf,
		reader: bufio.NewReader(fd),
	}, nil
}

func (r *SstReader) ReadFooter() error {
	_, err := r.fd.Seek(-int64(r.conf.SstFooterSize), io.SeekEnd)
	if err != nil {
		return err
	}
	filterOffset, err := binary.ReadUvarint(r.reader)
	if err != nil {
		return err
	}
	filterSize, err := binary.ReadUvarint(r.reader)
	if err != nil {
		return err
	}
	indexOffset, err := binary.ReadUvarint(r.reader)
	if err != nil {
		return err
	}
	indexSize, err := binary.ReadUvarint(r.reader)
	if err != nil {
		return err
	}
	if filterOffset == 0 || filterSize == 0 || indexOffset == 0 || indexSize == 0 {
		return fmt.Errorf("sst文件数据异常")
	}
	r.FilterOffset = int64(filterOffset)
	r.FilterSize = int64(filterSize)
	r.IndexOffset = int64(indexOffset)
	r.IndexSize = int64(indexSize)
	return nil
}
func (r *SstReader) readBlock(offset, size int64) ([]byte, error) {
	if _, err := r.fd.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	r.reader.Reset(r.fd)

	compress, err := r.read(size)
	if err != nil {
		return nil, err
	}
	crc := binary.LittleEndian.Uint32(compress[size-4:])
	compressData := compress[:size-4]

	if utils.Checksum(compressData) != crc {
		return nil, fmt.Errorf("数据块校验失败")
	}
	data, err := snappy.Decode(nil, compressData)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// 读取指定长度数据
func (r *SstReader) read(size int64) (b []byte, err error) {
	b = make([]byte, size)
	_, err = io.ReadFull(r.reader, b)
	return
}

func DecodeBlock(block []byte) ([]byte, []int) {
	n := len(block)

	nRestartPoint := int(binary.LittleEndian.Uint32(block[n-4:]))
	oRestartPoint := n - (nRestartPoint * 4) - 4
	restartPoint := make([]int, nRestartPoint)

	for i := 0; i < nRestartPoint; i++ {
		restartPoint[i] = int(binary.LittleEndian.Uint32(block[oRestartPoint+i*4:]))
	}
	return block[:oRestartPoint], restartPoint
}

// ReadRecord //读取变长字节三次，得到共享key长度，非共享key,值长度
func ReadRecord(prevKey []byte, buf *bytes.Buffer) ([]byte, []byte, error) {
	keyPrefixLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, nil, err
	}

	keyLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, nil, err
	}

	valueLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, nil, err
	}

	key := make([]byte, keyLen)
	_, err = io.ReadFull(buf, key)
	if err != nil {
		return nil, nil, err
	}

	value := make([]byte, valueLen)
	_, err = io.ReadFull(buf, value)
	if err != nil {
		return nil, nil, err
	}

	actualKey := make([]byte, keyPrefixLen)
	copy(actualKey, prevKey[0:keyPrefixLen])
	actualKey = append(actualKey, key...)
	return actualKey, value, nil
}

// ReadFilter 读取过滤块
func (r *SstReader) ReadFilter() (map[uint64][]byte, error) {
	if r.FilterOffset == 0 {
		if err := r.ReadFooter(); err != nil {
			return nil, err
		}
	}

	data, err := r.readBlock(r.FilterOffset, r.FilterSize)
	if err != nil {
		return nil, err
	}
	return ReadFilter(data), nil
}

// ReadFilter 解析过滤块为 偏移->布隆过滤器 map
func ReadFilter(index []byte) map[uint64][]byte {

	data, _ := DecodeBlock(index)
	buf := bytes.NewBuffer(data)

	filterMap := make(map[uint64][]byte, 0)
	prevKey := make([]byte, 0)

	for {
		key, value, err := ReadRecord(prevKey, buf)

		if err != nil {
			break
		}

		offset, _ := binary.Uvarint(key)
		filterMap[offset] = value
		prevKey = key
	}
	return filterMap
}
func ReadIndex(index []byte) []*Index {
	data, _ := DecodeBlock(index)
	indexBuf := bytes.NewBuffer(data)

	indexs := make([]*Index, 0)
	prevKey := make([]byte, 0)

	for {
		key, value, err := ReadRecord(prevKey, indexBuf)
		if err != nil {
			break
		}
		offset, n := binary.Uvarint(value)
		size, _ := binary.Uvarint(value[n:])

		indexs = append(indexs, &Index{
			Key:    key,
			Offset: uint64(offset),
			Size:   uint64(size),
		})
		prevKey = key
	}
	return indexs
}

func (r *SstReader) ReadIndex() ([]*Index, error) {
	if r.IndexOffset == 0 {
		if err := r.ReadFooter(); err != nil {
			return nil, err
		}
	}

	data, err := r.readBlock(r.IndexOffset, r.IndexSize)
	if err != nil {
		return nil, err
	}
	return ReadIndex(data), nil
}
