package state_machine

import (
	"fmt"
	"khanh/raft-go/extensions/etcd/common"
	"sync"
)

type WatcherCache struct {
	eventCache []common.EtcdResultRes
	// after the array is full,
	// we increase the base index by one to make space for the new item,
	// when then base index reach the end of the array, we do wrap around, set it to 0.
	baseIndex int
	lock      sync.RWMutex
}

func NewWatcherCache(capacity int) WatcherCache {
	return WatcherCache{
		eventCache: make([]common.EtcdResultRes, 0, capacity),
		baseIndex:  0,
	}
}

func (w *WatcherCache) empty() bool {
	w.lock.RLock()
	defer w.lock.RUnlock()

	return len(w.eventCache) == 0
}

func (w *WatcherCache) latest() (newData common.EtcdResultRes, ok bool) {
	w.lock.RLock()
	defer w.lock.RUnlock()

	if w.empty() {
		return newData, false
	}

	length, capacity := len(w.eventCache), cap(w.eventCache)
	if length < capacity {
		return w.eventCache[length-1], true
	} else {
		if w.baseIndex == 0 {
			return w.eventCache[capacity-1], true
		}
		return w.eventCache[w.baseIndex-1], true
	}
}

func (w *WatcherCache) oldest() (newData common.EtcdResultRes, ok bool) {
	w.lock.RLock()
	defer w.lock.RUnlock()

	if w.empty() {
		return newData, false
	}

	length, capacity := len(w.eventCache), cap(w.eventCache)
	if length < capacity {
		return w.eventCache[0], true
	} else {
		return w.eventCache[w.baseIndex], true
	}
}

// find and return the first occurrence of the key in cache,
// waitIndex has to be bigger than 0
func (w *WatcherCache) find(key string, waitIndex int) (common.EtcdResultRes, bool) {
	w.lock.RLock()
	defer w.lock.RUnlock()

	if w.empty() || waitIndex == 0 {
		return common.EtcdResultRes{}, false
	}

	length, capacity := len(w.eventCache), cap(w.eventCache)
	if length < capacity {
		for _, kv := range w.eventCache {
			if kv.Node.Key == key && kv.Node.ModifiedIndex >= waitIndex {
				return kv, true
			}
		}
	} else {
		for i := w.baseIndex; i < capacity; i++ {
			kv := w.eventCache[i]
			if kv.Node.Key == key && kv.Node.ModifiedIndex >= waitIndex {
				return kv, true
			}
		}

		for i := 0; i < w.baseIndex; i++ {
			kv := w.eventCache[i]
			if kv.Node.Key == key && kv.Node.ModifiedIndex >= waitIndex {
				return kv, true
			}
		}
	}

	return common.EtcdResultRes{}, false
}

func (w *WatcherCache) Get(key string, waitIndex int) (common.EtcdResultRes, bool, error) {
	w.lock.RLock()
	defer w.lock.RUnlock()

	if w.empty() || waitIndex == 0 {
		return common.EtcdResultRes{}, false, nil
	}

	latest, _ := w.latest()
	oldest, _ := w.oldest()

	if waitIndex < oldest.Node.ModifiedIndex {
		return common.EtcdResultRes{}, false, common.EtcdResultErr{
			ErrorCode: 401,
			Index:     latest.Node.ModifiedIndex,
			Message:   "The event in requested index is outdated and cleared",
			Cause:     fmt.Sprintf("the requested history has been cleared [%d/%d]", oldest.Node.ModifiedIndex, waitIndex),
		}
	}

	data, found := w.find(key, waitIndex)
	if found {
		return data, true, nil
	}
	return common.EtcdResultRes{}, false, nil
}

func (w *WatcherCache) Put(result common.EtcdResultRes) {
	w.lock.Lock()
	defer w.lock.Unlock()

	capacity := cap(w.eventCache)
	if len(w.eventCache) < capacity {
		w.eventCache = append(w.eventCache, result)
	} else {
		w.eventCache[w.baseIndex] = result
		w.baseIndex++
		if w.baseIndex >= capacity {
			w.baseIndex = 0
		}
	}
}
