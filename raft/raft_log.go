package raft

type RaftLog struct {
}

func (l *RaftLog) GetLastLogIndexAndTerm() (uint64, uint64) {
	//todo
	return 0, 0
}

func (l *RaftLog) Apply(commit uint64, index uint64) {

}
