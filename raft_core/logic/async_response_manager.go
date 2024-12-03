package logic

import (
	"fmt"
	gc "khanh/raft-go/common"
	"net/http"
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
		return gc.RaftError{
			Message:  fmt.Sprintf("already registered log index: %d", logIndex),
			HttpCode: http.StatusBadRequest,
		}
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

func (a *AsyncResponseManager) PutResponse(logIndex int, msg gc.LogResult, resErr error, timeout time.Duration) error {
	a.lock.Lock()
	index := AsyncResponseIndex{logIndex}

	slot, ok := a.m[index]
	if !ok {
		a.lock.Unlock()

		return gc.RaftError{Message: fmt.Sprintf("register log index: %d first", logIndex)}
	}

	a.lock.Unlock()

	select {
	case slot.msg <- AsyncResponseItem{Response: msg, Err: resErr}:
		log.Info().Int("log index", logIndex).Interface("msg", msg).Interface("err", resErr).Msg("PutResponse")
	case <-time.After(timeout):
		return gc.RaftError{
			Message:  fmt.Sprintf("channel log index: %d is not empty", logIndex),
			HttpCode: http.StatusInternalServerError,
		}
	}

	return nil
}

// blocking call
func (a *AsyncResponseManager) TakeResponse(logIndex int, timeout time.Duration) (gc.LogResult, error) {
	a.lock.RLock()

	index := AsyncResponseIndex{logIndex}
	item, ok := a.m[index]
	if !ok {
		a.lock.RUnlock()

		return "", gc.RaftError{
			Message:  fmt.Sprintf("register log index: %d first", logIndex),
			HttpCode: http.StatusBadRequest,
		}
	}

	a.lock.RUnlock()

	select {
	case res := <-item.msg:
		close(item.msg)
		delete(a.m, index)
		return res.Response, res.Err
	case <-time.After(timeout):
		return "", gc.RaftError{
			Message:  "timeout error: can't get message",
			HttpCode: http.StatusInternalServerError,
		}
	}
}
