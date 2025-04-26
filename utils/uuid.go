package utils

import (
	"crypto/rand"
	"io"
	"log"
)

var UuidChan = make(chan []byte, 1000)

func UuidFactory() {
	for {
		UuidChan <- uuid()
	}
}

func uuid() []byte {
	b := make([]byte, 16)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		log.Fatal(err)
	}
	return b
}

func UuidToString(Uuid []byte) string {
	return string(Uuid)
}
