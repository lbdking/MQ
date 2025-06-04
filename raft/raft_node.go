package raft

import (
	"MQ/pb"
	"context"
	"go.uber.org/zap"
	"time"
)

type RaftNode struct {
	raft   *Raft                  //raft实例
	recvc  chan *pb.RaftMessage   //一般消息接收通道
	propc  chan *pb.RaftMessage   //提议消息接受通道
	sendc  chan []*pb.RaftMessage //消息发送通道
	stopc  chan struct{}          //停止
	ticker *time.Ticker           //定时器
	logger *zap.SugaredLogger
}

func NewRaftNode(id uint64, peers map[uint64]string, logger *zap.SugaredLogger) *RaftNode {
	node := &RaftNode{
		raft:   NewRaft(id, peers, logger),
		recvc:  make(chan *pb.RaftMessage),
		propc:  make(chan *pb.RaftMessage),
		sendc:  make(chan []*pb.RaftMessage),
		stopc:  make(chan struct{}),
		ticker: time.NewTicker(time.Second),
		logger: logger,
	}
	node.Start()
	return node
}
func (n *RaftNode) Start() {
	go func() {
		//var propc chan *pb.RaftMessage
		var sendc chan []*pb.RaftMessage

		for {
			var msgs []*pb.RaftMessage

			if len(n.raft.Msg) > 0 {
				msgs = n.raft.Msg
				sendc = n.sendc
			} else {
				sendc = nil
			}

			select {
			case <-n.ticker.C:
				n.raft.Tick()
			case msg := <-n.recvc:
				n.raft.HandleMessage(msg)
			case msg := <-n.propc:
				n.raft.HandleMessage(msg)
			case sendc <- msgs:
				n.raft.Msg = n.raft.Msg[len(msgs):]
			case <-n.stopc:
				return
			}
		}
	}()
}

// Process 根据消息类型不同，将消息放入对应的通道中
func (n *RaftNode) Process(ctx context.Context, msg *pb.RaftMessage) error {
	var ch chan *pb.RaftMessage
	if msg.Type == pb.MessageType_PROPOSE {
		ch = n.propc
	} else {
		ch = n.recvc
	}
	select {
	case ch <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *RaftNode) SendChan() chan []*pb.RaftMessage {
	return n.sendc
}
