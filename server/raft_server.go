package server

import (
	"MQ/pb"
	"MQ/raft"
	"context"
	"fmt"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"io"
	"net"
	"strconv"
)

type RaftServer struct {
	pb.RaftServer
	id          uint64       // 服务器的唯一标识符，使用无符号 64 位整数表示
	name        string       // 服务器的名称
	peerAddress string       // 服务器的对等节点地址
	raftServer  *grpc.Server // gRPC 服务器实例，用于处理 Raft 相关的 RPC 请求

	tmpPeers     map[uint64]*Peer
	incomingChan chan *pb.RaftMessage // 用于接收传入的 Raft 消息的通道
	peers        map[uint64]*Peer     // 存储对等节点信息的映射，键为对等节点的 ID，值为 Peer 类型的指针
	node         *raft.RaftNode       // Raft 节点实例
	close        bool
	stopc        chan struct{}
	metric       chan pb.MessageType
	logger       *zap.SugaredLogger
}

func (s *RaftServer) Start() {
	lis, err := net.Listen("tcp", s.peerAddress)
	if err != nil {
		s.logger.Errorf("对等节点服务器失败: %v ", err)
	}
	var opt []grpc.ServerOption
	s.raftServer = grpc.NewServer(opt...)

	s.logger.Infof("启动对等节点服务器 %s", s.name)

	pb.RegisterRaftServer(s.raftServer, s)

	s.handle()

	err = s.raftServer.Serve(lis)
	if err != nil {
		s.logger.Errorf("Raft 服务器关闭: %v", err)
	}
}

func (s *RaftServer) Consensus(stream pb.Raft_ConsensusServer) error {
	msg, err := stream.Recv()
	if err == io.EOF {
		s.logger.Debugf("关闭 %s 流", strconv.FormatUint(s.id, 16))
	}
	if err != nil {
		s.logger.Errorf("接收 %s 流消息失败: %v", strconv.FormatUint(s.id, 16), err)
	}
	return s.addServerPeer(stream, msg)
}

func (s *RaftServer) addServerPeer(stream pb.Raft_ConsensusServer, msg *pb.RaftMessage) error {
	p, isMember := s.peers[msg.From]
	if !isMember {
		s.logger.Debugf("收到非集群节点 %s 消息 %s", strconv.FormatUint(msg.From, 16), msg.String())
		p = NewPeer(msg.From, "", s.incomingChan, s.metric, s.logger)
		s.peers[msg.From] = p
		s.tmpPeers[msg.From] = p
		s.node.Process(context.Background(), msg)
		p.Recv()
		return fmt.Errorf("节点 %s 不在集群中", strconv.FormatUint(msg.From, 16))
	}

	s.logger.Debugf("添加 %s 读写流", strconv.FormatUint(msg.From, 16))
	if p.SetStream(stream) {
		// 处理消息
		s.node.Process(context.Background(), msg)
		//启动接受消息的协程
		p.Recv()
	}
	return nil
}

func (s *RaftServer) handle() {
	go func() {
		for {
			select {
			case <-s.stopc:
				s.logger.Debugf("关闭 %s 流", strconv.FormatUint(s.id, 16))
				return
			case msgs := <-s.node.SendChan():
				s.sendMsg(msgs)
			case msg := <-s.incomingChan:
				s.process(msg)
			}

		}
	}()
}

func (s *RaftServer) sendMsg(msgs []*pb.RaftMessage) {
	msgMap := make(map[uint64][]*pb.RaftMessage, len(s.peers)-1)

	for _, msg := range msgs {
		if s.peers[msg.To] == nil {
			p := s.tmpPeers[msg.To]
			if p != nil {
				p.send(msg)
			}
			p.Stop()
			delete(s.tmpPeers, msg.To)
			continue
		} else {
			if msgMap[msg.To] == nil {
				msgMap[msg.To] = make([]*pb.RaftMessage, 0)
			}
			msgMap[msg.To] = append(msgMap[msg.To], msg)
		}
	}
	for k, v := range msgMap {
		if len(v) > 0 {
			s.peers[k].SendBatch(v)
		}
	}
}

func (s *RaftServer) process(msg *pb.RaftMessage) (err error) {
	defer func() {
		if reason := recover(); reason != nil {
			err = fmt.Errorf("处理消息 %s 失败 %s", msg.String(), reason)
			s.logger.Errorf("处理 %s 消息失败: %v", msg.String(), reason)
		}
	}()
	return s.node.Process(context.Background(), msg)
}
