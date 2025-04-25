package utils

type ChanReq struct {
	Variable interface{}
	//接收响应的通道
	RetChan chan interface{}
}

type ChanRet struct {
	Err      error
	Variable interface{}
}
