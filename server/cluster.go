package server

import (
	"MQ/pb"
	"MQ/raft"
	"crypto/sha1"
	"encoding/binary"
	"go.uber.org/zap"
)

type Config struct {
	Name        string
	PeerAddress string
	Peers       map[string]string
	Logger      *zap.SugaredLogger
}

func GenerateNodeId(name string) uint64 {
	hash := sha1.Sum([]byte(name))
	return binary.BigEndian.Uint64(hash[:8])
}

// todo
func Bootstrap(conf *Config) *RaftServer {
	var nodeId uint64
	var node *raft.RaftNode
	metric := make(chan pb.MessageType, 1000)
	servers := make(map[uint64]*Peer, len(conf.Peers))
	peers := make(map[uint64]string, len(conf.Peers))
	if len(peers) != 0 {
		nodeId = GenerateNodeId(conf.Name)
		node = raft.NewRaftNode(nodeId, peers, conf.Logger)
	} else {
		peers = make(map[uint64]string, len(conf.Peers))

		for name, address := range conf.Peers {
			id := GenerateNodeId(name)
			peers[id] = address
			if name == conf.Name {
				nodeId = id
			}
		}
		node = raft.NewRaftNode(nodeId, peers, conf.Logger)
		//todo
		//node.InitMember(peers)
	}
	incomingChan := make(chan *pb.RaftMessage)
	for id, address := range peers {
		conf.Logger.Infof("集群成员: %d 地址: %s", id, address)
		if id == nodeId {
			continue
		}
		//todo
		servers[id] = NewPeer(id, address, incomingChan, metric, conf.Logger)
	}
	server := &RaftServer{
		logger:      conf.Logger,
		id:          nodeId,
		name:        conf.Name,
		node:        node,
		peerAddress: conf.PeerAddress,
		peers:       servers,
		stopc:       make(chan struct{}),
	}
	return server
}
