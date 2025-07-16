package wal

import (
	"MQ/utils"
	"bytes"
	"encoding/binary"
	"go.uber.org/zap"
	"os"
	"sync"
)

const (
	kFull = iota
	kFirst
	kMiddle
	kLast
)

type WalWriter struct {
	mu            sync.RWMutex
	dir           string
	seqNo         int
	fd            *os.File
	header        [20]byte
	buf           *bytes.Buffer
	prevBlockType uint8
	logger        *zap.SugaredLogger
}

func (w *WalWriter) Write(key, value []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()

	n := binary.PutUvarint(w.header[0:], uint64(len(key)))
	n += binary.PutUvarint(w.header[n:], uint64(len(value)))

	length := len(key) + len(value) + n
	b := make([]byte, length)
	copy(b, w.header[:n])
	copy(b[n:], key)
	copy(b[n+len(key):], value)

	size := walBlockSize - w.buf.Len()
	if size < length {
		w.buf.Write(b[:size])
		w.PaddingBlock(size-length, false)
		w.buf.Write(b[size:])
	} else {
		w.buf.Write(b)
		w.PaddingBlock(size-length, false)
	}
}

func (w *WalWriter) PaddingBlock(remian int, force bool) {
	var blockType uint8
	if remian < 0 {
		if w.prevBlockType == kFirst || w.prevBlockType == kMiddle {
			blockType = kMiddle
		} else {
			blockType = kFirst
		}
		w.WriteBlock(blockType, uint16(w.buf.Len()-7))
		w.prevBlockType = blockType
	} else if remian < 7 || force {
		w.buf.Write(make([]byte, remian))
		if w.prevBlockType == kFirst || w.prevBlockType == kMiddle {
			blockType = kLast
		} else {
			blockType = kFull
		}
		w.WriteBlock(blockType, uint16(w.buf.Len()-remian-7))
		w.prevBlockType = blockType
	}
}

func (w *WalWriter) WriteBlock(blockType uint8, length uint16) {
	data := w.buf.Bytes()
	binary.LittleEndian.PutUint16(w.header[4:6], length)
	data[6] = byte(blockType)
	crc := utils.Checksum(data[4:])
	binary.LittleEndian.PutUint32(data[:4], crc)
	w.fd.Write(data)
	w.buf.Truncate(7)
}
