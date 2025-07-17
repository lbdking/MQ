package wal

import (
	"MQ/skiplist"
	"MQ/utils"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const walBlockSize = 32 * 1024

type WalReader struct {
	fd    *os.File
	block []byte
	data  []byte
	buf   *bytes.Buffer
}

func (r *WalReader) Read() error {
	_, err := io.ReadFull(r.fd, r.block)
	if err != nil {
		return err
	}
	return nil
}
func ReadRecord(buf *bytes.Buffer) (key, value []byte, err error) {
	keyLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, nil, err
	}
	valueLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, nil, err
	}
	key = make([]byte, keyLen)
	_, err = io.ReadFull(buf, key)
	if err != nil {
		return nil, nil, err
	}
	value = make([]byte, valueLen)
	_, err = io.ReadFull(buf, value)
	if err != nil {
		return nil, nil, err
	}
	return key, value, nil
}

// Next wal键值对遍历
/*
 -循环读取块，直到结束或者得到完整数据
  -先进性块crc校验，失败返回
  -成功继续读取块，直到得到完整数据或者块结束
 -解码读取键值对，成功返回，调用Next()继续读取下一个键值对
*/
func (r *WalReader) Next() (key, value []byte, err error) {
	var prevBlockType uint8
	for r.buf == nil {
		err := r.Read()
		if err != nil {
			if err == io.EOF {
				return nil, nil, nil
			}
			return nil, nil, fmt.Errorf("读取预写日志块失败：%v", err)
		}
		crc := binary.LittleEndian.Uint32(r.block[:4])
		length := binary.LittleEndian.Uint16(r.block[4:6])
		blockType := uint8(r.block[6])

		if crc == utils.Checksum(r.block[4:]) {
			switch blockType {
			case kFull:
				r.data = r.block[7 : length+7]
				r.buf = bytes.NewBuffer(r.data)
			case kFirst:
				r.data = make([]byte, length)
				copy(r.data, r.block[7:length+7])
			case kMiddle:
				if prevBlockType == kMiddle || prevBlockType == kFirst {
					d := r.block[7 : length+7]
					r.data = append(r.data, d...)
				}
			case kLast:
				if prevBlockType == kMiddle || prevBlockType == kFirst {
					r.data = append(r.data, r.block[7:length+7]...)
					r.buf = bytes.NewBuffer(r.data)
				}
			}
			prevBlockType = blockType
		} else {
			return nil, nil, fmt.Errorf("预写日志校验失败")
		}
	}
	key, value, err = ReadRecord(r.buf)
	if err == nil {
		return key, value, nil
	}
	if err != io.EOF {
		return nil, nil, fmt.Errorf("预写日志读取失败: %v", err)
	}
	r.buf = nil
	return r.Next()
}

func (r *WalReader) Close() {
	r.block = nil
	r.data = nil
	err := r.fd.Close()
	if err != nil {
		return
	}
}
func NewWalReader(fd *os.File) *WalReader {
	return &WalReader{
		fd:    fd,
		block: make([]byte, walBlockSize),
	}
}
func Restore(walFile string) (*skiplist.SkipList, error) {
	fd, err := os.OpenFile(walFile, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("打开 %s.wal 失败: %v", walFile, err)
	}
	sl := skiplist.NewSkipList()
	r := NewWalReader(fd)
	defer r.Close()
	for {
		k, v, err := r.Next()
		if err != nil {
			return sl, err
		}
		if len(k) == 0 {
			break
		}
		sl.Put(k, v)
	}
	return sl, nil
}
