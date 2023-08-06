package logic

type Session struct {
	LastSequenceNum int
	LastResponse    any
}

type SimpleSessionManager struct {
	data map[int]Session // session id -> session info
	size int
}

func NewSimpleSessionManager(size int) SimpleSessionManager {
	return SimpleSessionManager{
		data: make(map[int]Session),
		size: size,
	}
}
