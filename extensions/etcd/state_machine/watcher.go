package state_machine

import (
	"khanh/raft-go/extensions/etcd/common"
	"sync"
)

type Watcher struct {
	keyWatch    map[string]map[int]WatchRequest
	prefixWatch map[string]map[int]WatchRequest
	lock        sync.RWMutex
	nextId      int

	cache WatcherCache
}

type WatchRequest struct {
	WaitIndex int
	Channel   common.EtcdResultPromise
}

func NewWatcher(responseCacheCapacity int) Watcher {
	return Watcher{
		keyWatch:    make(map[string]map[int]WatchRequest),
		prefixWatch: make(map[string]map[int]WatchRequest),
		lock:        sync.RWMutex{},
		nextId:      0,
		cache:       NewWatcherCache(responseCacheCapacity),
	}
}

func (w *Watcher) Register(key string, isPrefix bool, waitIndex int) (id int, channel common.EtcdResultPromise, err error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if waitIndex > 0 {
		resData, found, err := w.cache.Get(key, waitIndex)
		if err != nil {
			return 0, nil, err
		}

		// check data empty
		if found {
			channel = make(chan common.EtcdResultRes, 1)
			defer func() {
				channel <- resData
			}()
			return 0, channel, nil
		}
	}

	w.nextId++

	if isPrefix {
		if _, ok := w.prefixWatch[key]; !ok {
			w.prefixWatch[key] = map[int]WatchRequest{}
		}
		channel = make(chan common.EtcdResultRes, 1)
		w.prefixWatch[key][w.nextId] = WatchRequest{WaitIndex: waitIndex, Channel: channel}
	} else {
		if _, ok := w.keyWatch[key]; !ok {
			w.keyWatch[key] = map[int]WatchRequest{}
		}
		channel = make(chan common.EtcdResultRes, 1)
		w.keyWatch[key][w.nextId] = WatchRequest{WaitIndex: waitIndex, Channel: channel}
	}

	return w.nextId, channel, nil
}

func (w *Watcher) Notify(newData common.EtcdResultRes) {
	w.lock.RLock()
	defer w.lock.RUnlock()

	w.cache.Put(newData)

	key := newData.Node.Key
	if subscribers, ok := w.keyWatch[key]; ok {
		for id, promise := range subscribers {
			if promise.WaitIndex <= newData.Node.ModifiedIndex {
				promise.Channel <- newData
				delete(w.keyWatch[key], id)
			}
		}
		if len(w.keyWatch[key]) == 0 {
			delete(w.keyWatch, key)
		}
	}

	// this approach is bad when key is long,
	// or we can search concurrently
	prefix := key
	for prefix != "" {
		if subscribers, ok := w.prefixWatch[prefix]; ok {
			for id, promise := range subscribers {
				if promise.WaitIndex <= newData.Node.ModifiedIndex {
					promise.Channel <- newData
					delete(w.prefixWatch[prefix], id)
				}
			}
			if len(w.prefixWatch[prefix]) == 0 {
				delete(w.prefixWatch, prefix)
			}
		}

		prefix = prefix[:len(prefix)-1] // remove last character
	}
}
