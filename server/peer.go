package server

import (
	"MQ/pb"
	"MQ/raft"
	"context"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"strconv"
	"sync"
)

type Stream interface {
	Send(*pb.RaftMessage) error
	Recv() (*pb.RaftMessage, error)
}

type Peer struct {
	mu     sync.Mutex
	wg     sync.WaitGroup
	id     uint64
	node   *raft.RaftNode       //raft节点实例
	stream Stream               //grpc双向流
	recvc  chan *pb.RaftMessage //流读取数据发送通道
	remote *Remote              //远端连接消息
	close  bool                 //是否关闭
	logger *zap.SugaredLogger
}

type Remote struct {
	address      string
	conn         *grpc.ClientConn
	client       pb.RaftClient
	clientStream pb.Raft_ConsensusClient
	serverStream pb.Raft_ConsensusServer
}

// todo
func NewPeer(from uint64, s string, incomingChan chan *pb.RaftMessage, metric chan pb.MessageType, logger *zap.SugaredLogger) *Peer {
	return nil
}

func (p *Peer) send(msg *pb.RaftMessage) {
	if msg == nil {
		return
	}
	if p.stream == nil {
		if err := p.Connect(); err != nil {
			return
		}
	}
	if err := p.stream.Send(msg); err != nil {
		p.logger.Errorf("发送消息 %s 到 %s 失败， 日志数量：%d %v", msg.Type.String(), strconv.FormatUint(msg.To, 16), len(msg.Entry), err)
		return
	}
}

func (p *Peer) SendBatch(msg []*pb.RaftMessage) {
	p.wg.Add(1)
	var appEntryMsg *pb.RaftMessage
	var propMsg *pb.RaftMessage
	for _, msg := range msg {
		if msg.Type == pb.MessageType_APPEND_ENTRY {
			if appEntryMsg == nil {
				appEntryMsg = msg
			} else {
				size := len(appEntryMsg.Entry)
				if size == 0 || len(msg.Entry) == 0 || appEntryMsg.Entry[size-1].Index+1 == msg.Entry[0].Index {
					appEntryMsg.LastCommit = msg.LastCommit
					appEntryMsg.Entry = append(appEntryMsg.Entry, msg.Entry...)
				} else if appEntryMsg.Entry[0].Index >= msg.Entry[0].Index {
					appEntryMsg = msg
				}
			}
		} else if msg.Type == pb.MessageType_PROPOSE {
			if propMsg == nil {
				propMsg = msg
			} else {
				propMsg.Entry = append(propMsg.Entry, msg.Entry...)
			}
		} else {
			p.send(msg)
		}
	}

	if appEntryMsg != nil {
		p.send(appEntryMsg)
	}

	if propMsg != nil {
		p.send(propMsg)
	}
	p.wg.Done()
}
func (p *Peer) Connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stream != nil {
		return nil
	}

	if p.remote.conn == nil {
		var opts []grpc.DialOption
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

		conn, err := grpc.Dial(p.remote.address, opts...)
		if err != nil {
			p.logger.Errorf("连接 %s 失败 %v", p.remote.address, err)
			return err
		}
		p.remote.conn = conn
		p.remote.client = pb.NewRaftClient(conn)
	}

	return p.Reconnect()
}

func (p *Peer) SetStream(stream pb.Raft_ConsensusServer) bool {
	/*
	 -如果当前节点的流为空，则设置并关闭原本可能存在的流
	 -如果当前节点的流不为空，则不进行设置，并返回false
	*/
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stream == nil {
		p.stream = stream
		p.remote.serverStream = stream

		if p.remote.clientStream != nil {
			p.remote.clientStream.CloseSend()
			p.remote.conn.Close()
			p.remote.clientStream = nil
			p.remote.conn = nil
		}

		return true
	}
	return false
}

func (p *Peer) Reconnect() error {

	if p.remote.clientStream != nil {
		p.remote.clientStream.CloseSend()
		p.remote.clientStream = nil
		p.stream = nil
	}

	stream, err := p.remote.client.Consensus(context.Background())
	// var delay time.Duration
	for err != nil {
		p.logger.Errorf("连接raft服务 %s 失败: %v", p.remote.address, err)
		return err
	}

	p.logger.Debugf("创建 %s 读写流", strconv.FormatUint(p.id, 16))
	p.stream = stream
	p.remote.clientStream = stream

	go p.Recv()
	return nil
}

// Recv 接收消息
func (p *Peer) Recv() {
	for {
		mgs, err := p.stream.Recv()
		if err == io.EOF {
			p.stream = nil
			p.logger.Errorf("关闭 %s 流", strconv.FormatUint(p.id, 16))
			return
		}
		if err != nil {
			p.logger.Errorf("接收 %s 流消息失败: %v", strconv.FormatUint(p.id, 16), err)
			return
		}
		p.recvc <- mgs
	}
}

func (p *Peer) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.wg.Wait()
	p.logger.Infof("停止 %s 流", strconv.FormatUint(p.id, 16))

	if p.remote.clientStream != nil {
		p.remote.clientStream.CloseSend()
	}
	if p.remote.conn != nil {
		p.remote.conn.Close()
	}
}
