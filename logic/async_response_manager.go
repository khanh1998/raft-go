package logic

import (
	"fmt"
	"khanh/raft-go/common"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// AsyncResponseManager store the result after the state machine process a log
type AsyncResponseManager struct {
	m    map[AsyncResponseIndex]AsyncResponse // log index -> response
	size int
	lock sync.RWMutex
}

func NewAsyncResponseManager(size int) AsyncResponseManager {
	return AsyncResponseManager{m: map[AsyncResponseIndex]AsyncResponse{}, size: size}
}

func (a *AsyncResponseManager) Register(logIndex int) error {
	a.lock.Lock()
	defer a.lock.Unlock()

	index := AsyncResponseIndex{logIndex}

	_, ok := a.m[index]
	if ok {
		return fmt.Errorf("already registered log index: %d", logIndex)
	}

	a.m[index] = AsyncResponse{
		msg:       make(chan AsyncResponseItem, 3),
		createdAt: time.Now(),
	}

	log.Info().
		Interface("data", a.m[index]).
		Interface("index", logIndex).
		Interface("capacity", cap(a.m[index].msg)).
		Msg("Register")

	return nil
}

func (a *AsyncResponseManager) PutResponse(logIndex int, msg common.LogResult, resErr error, timeout time.Duration) error {
	a.lock.Lock()
	index := AsyncResponseIndex{logIndex}

	slot, ok := a.m[index]
	if !ok {
		a.lock.Unlock()

		return fmt.Errorf("register log index: %d first", logIndex)
	}

	a.lock.Unlock()

	select {
	case slot.msg <- AsyncResponseItem{Response: msg, Err: resErr}:
		log.Info().Int("log index", logIndex).Interface("msg", msg).Interface("err", resErr).Msg("PutResponse")
	case <-time.After(timeout):
		return fmt.Errorf("channel log index: %d is not empty", logIndex)
	}

	return nil
}

// blocking call
func (a *AsyncResponseManager) TakeResponse(logIndex int, timeout time.Duration) (common.LogResult, error) {
	a.lock.RLock()

	index := AsyncResponseIndex{logIndex}
	item, ok := a.m[index]
	if !ok {
		a.lock.RUnlock()

		return "", fmt.Errorf("register log index: %d first", logIndex)
	}

	a.lock.RUnlock()

	select {
	case res := <-item.msg:
		close(item.msg)
		delete(a.m, index)
		return res.Response, res.Err
	case <-time.After(timeout):
		return "", fmt.Errorf("timeout error: can't get message")
	}
}
