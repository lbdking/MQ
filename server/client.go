package server

import (
	"encoding/binary"
	"io"
	"log"
)

type Client struct {
	//表示一个可以进行读写操作并且可以关闭的连接
	conn  io.ReadWriteCloser
	name  string
	state int
}

// NewClient 初始化client的状态为-1
func NewClient(conn io.ReadWriteCloser, name string) *Client {
	return &Client{conn: conn, name: name, state: -1}
}

func (c *Client) String() string {
	return c.name
}

func (c *Client) GetState() int {
	return c.state
}

func (c *Client) SetState(state int) {
	c.state = state
}

func (c *Client) Close() {
	log.Printf("Client Name(%s): closing\n", c.String())
	err := c.conn.Close()
	if err != nil {
		return
	}
}

// Read n为读取的字节数，err为返回的错误
func (c *Client) Read(data []byte) (n int, err error) {
	n, err = c.conn.Read(data)
	if err != nil {
		log.Printf("Client Name(%s): read error: %s\n", c.String(), err)
		return
	}
	return
}

func (c *Client) Write(data []byte) (n int, err error) {
	//将数据长度以大段字节序写入连接，固定为int32，4字节
	err = binary.Write(c.conn, binary.BigEndian, int32(len(data)))
	if err != nil {
		log.Printf("Client Name(%s): write error: %s\n", c.String(), err)
		return 0, err
	}

	//将数据写入连接
	n, err = c.conn.Write(data)
	if err != nil {
		log.Printf("Client Name(%s): write error: %s\n", c.String(), err)
		return 0, err
	}

	return n + 4, nil
}
