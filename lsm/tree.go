package lsm

import "go.uber.org/zap"

type Config struct {
	Dir                 string
	Logger              *zap.SugaredLogger
	MaxLevel            int //最大层数
	SstSize             int //sst文件大小
	SstDataBlockSize    int //sst数据块大小
	SstFooterSize       int //sst尾大小
	SstBlockTrailerSize int //Sst块尾大小
	SsRestartInterval   int //sst重启点间隔
}
type Tree struct {
}
